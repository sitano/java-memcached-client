/**
 * Copyright (C) 2006-2009 Dustin Sallings
 * Copyright (C) 2009-2011 Couchbase, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALING
 * IN THE SOFTWARE.
 */

package net.spy.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import net.spy.memcached.auth.AuthDescriptor;
import net.spy.memcached.auth.AuthThreadMonitor;
import net.spy.memcached.compat.SpyObject;
import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.BulkGetFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.SingleElementInfiniteIterator;
import net.spy.memcached.ops.*;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.TranscodeService;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.StringUtils;

/**
 * Client to a memcached server.
 *
 * <h2>Basic usage</h2>
 *
 * <pre>
 * MemcachedClient c = new MemcachedClient(
 *    new InetSocketAddress(&quot;hostname&quot;, portNum));
 *
 * // Store a value (async) for one hour
 * c.set(&quot;someKey&quot;, 3600, someObject);
 * // Retrieve a value.
 * Object myObject = c.get(&quot;someKey&quot;);
 * </pre>
 *
 * <h2>Advanced Usage</h2>
 *
 * <p>
 * MemcachedClient may be processing a great deal of asynchronous messages or
 * possibly dealing with an unreachable memcached, which may delay processing.
 * If a memcached is disabled, for example, MemcachedConnection will continue to
 * attempt to reconnect and replay pending operations until it comes back up. To
 * prevent this from causing your application to hang, you can use one of the
 * asynchronous mechanisms to time out a request and cancel the operation to the
 * server.
 * </p>
 *
 * <pre>
 *      // Get a memcached client connected to several servers
 *      // over the binary protocol
 *      MemcachedClient c = new MemcachedClient(new BinaryConnectionFactory(),
 *              AddrUtil.getAddresses("server1:11211 server2:11211"));
 *
 *      // Try to get a value, for up to 5 seconds, and cancel if it
 *      // doesn't return
 *      Object myObj = null;
 *      Future&lt;Object&gt; f = c.asyncGet("someKey");
 *      try {
 *          myObj = f.get(5, TimeUnit.SECONDS);
 *      // throws expecting InterruptedException, ExecutionException
 *      // or TimeoutException
 *      } catch (Exception e) {  /*  /
 *          // Since we don't need this, go ahead and cancel the operation.
 *          // This is not strictly necessary, but it'll save some work on
 *          // the server.  It is okay to cancel it if running.
 *          f.cancel(true);
 *          // Do other timeout related stuff
 *      }
 * </pre>
 *
 * <p>Optionally, it is possible to activate a check that makes sure that
 * the node is alive and responding before running actual operations (even
 * before authentication. Only enable this if you are sure that you do not
 * run into issues during connection (some memcached services have problems
 * with it). You can enable it by setting the net.spy.verifyAliveOnConnect
 * System Property to "true".</p>
 */
public class MemcachedClient extends SpyObject implements MemcachedClientIF,
    ConnectionObserver {

  protected static final OperationStatus STATUS_ERROR = new OperationStatus(false, "Error");

  protected volatile boolean shuttingDown = false;

  protected final long operationTimeout;

  protected final MemcachedConnection mconn;

  protected final OperationFactory opFact;

  protected final Transcoder<Object> transcoder;

  protected final TranscodeService tcService;

  protected final AuthDescriptor authDescriptor;

  protected final ConnectionFactory connFactory;

  protected final AuthThreadMonitor authMonitor = new AuthThreadMonitor();

  protected final MemcachedClientIF quiet;

  /**
   * Get a memcache client operating on the specified memcached locations.
   *
   * @param ia the memcached locations
   * @throws IOException if connections cannot be established
   */
  public MemcachedClient(InetSocketAddress... ia) throws IOException {
    this(new DefaultConnectionFactory(), Arrays.asList(ia));
  }

  /**
   * Get a memcache client over the specified memcached locations.
   *
   * @param addrs the socket addrs
   * @throws IOException if connections cannot be established
   */
  public MemcachedClient(List<InetSocketAddress> addrs) throws IOException {
    this(new DefaultConnectionFactory(), addrs);
  }

  /**
   * Get a memcache client over the specified memcached locations.
   *
   * @param cf the connection factory to configure connections for this client
   * @param addrs the socket addresses
   * @throws IOException if connections cannot be established
   */
  public MemcachedClient(ConnectionFactory cf, List<InetSocketAddress> addrs)
    throws IOException {
    if (cf == null) {
      throw new NullPointerException("Connection factory required");
    }
    if (addrs == null) {
      throw new NullPointerException("Server list required");
    }
    if (addrs.isEmpty()) {
      throw new IllegalArgumentException("You must have at least one server to"
          + " connect to");
    }
    if (cf.getOperationTimeout() <= 0) {
      throw new IllegalArgumentException("Operation timeout must be positive.");
    }
    connFactory = cf;
    tcService = new TranscodeService(cf.isDaemon());
    transcoder = cf.getDefaultTranscoder();
    opFact = cf.getOperationFactory();
    assert opFact != null : "Connection factory failed to make op factory";
    mconn = cf.createConnection(addrs);
    assert mconn != null : "Connection factory failed to make a connection";
    operationTimeout = cf.getOperationTimeout();
    authDescriptor = cf.getAuthDescriptor();
    if (authDescriptor != null) {
      addObserver(this);
    }
    quiet = new MemcachedQuietClient(this);
  }

  public long getOperationTimeout() {
    return operationTimeout;
  }

  public MemcachedConnection getMemcachedConnection() {
    return mconn;
  }

  protected OperationFactory getOperationFactory() {
    return opFact;
  }

  public TranscodeService getTranscodeService() {
    return tcService;
  }

  /**
   * Get the addresses of available servers.
   *
   * <p>
   * This is based on a snapshot in time so shouldn't be considered completely
   * accurate, but is a useful for getting a feel for what's working and what's
   * not working.
   * </p>
   *
   * @return point-in-time view of currently available servers
   */
  @Override
  public Collection<SocketAddress> getAvailableServers() {
    ArrayList<SocketAddress> rv = new ArrayList<SocketAddress>();
    for (MemcachedNode node : mconn.getLocator().getAll()) {
      if (node.isActive()) {
        rv.add(node.getSocketAddress());
      }
    }
    return rv;
  }

  /**
   * Get the addresses of unavailable servers.
   *
   * <p>
   * This is based on a snapshot in time so shouldn't be considered completely
   * accurate, but is a useful for getting a feel for what's working and what's
   * not working.
   * </p>
   *
   * @return point-in-time view of currently available servers
   */
  @Override
  public Collection<SocketAddress> getUnavailableServers() {
    ArrayList<SocketAddress> rv = new ArrayList<SocketAddress>();
    for (MemcachedNode node : mconn.getLocator().getAll()) {
      if (!node.isActive()) {
        rv.add(node.getSocketAddress());
      }
    }
    return rv;
  }

  /**
   * Get a read-only wrapper around the node locator wrapping this instance.
   *
   * @return this instance's NodeLocator
   */
  @Override
  public NodeLocator getNodeLocator() {
    return mconn.getLocator().getReadonlyCopy();
  }

  /**
   * Get a memcached client instance to operate on a single node specified by it's group key
   * @param key Group key to select the node to operate
   * @return Client instance with operational node selected
   */
  @Override
  public MemcachedClientIF getGroupKey(String key) {
    StringUtils.validateKey(key, opFact instanceof BinaryOperationFactory);
    return new MemcachedGroupKeyClient(this, key);
  }

  @Override
  public MemcachedClientIF getQuietClient() {
    return quiet;
  }

  /**
   * Get the default transcoder that's in use.
   *
   * @return this instance's Transcoder
   */
  @Override
  public Transcoder<Object> getTranscoder() {
    return transcoder;
  }

  public CountDownLatch broadcastOp(final BroadcastOpFactory of) {
    return broadcastOp(of, mconn.getLocator().getAll(), true);
  }

  public CountDownLatch broadcastOp(final BroadcastOpFactory of,
      Collection<MemcachedNode> nodes) {
    return broadcastOp(of, nodes, true);
  }

  private CountDownLatch broadcastOp(BroadcastOpFactory of,
      Collection<MemcachedNode> nodes, boolean checkShuttingDown) {
    if (checkShuttingDown && shuttingDown) {
      throw new IllegalStateException("Shutting down");
    }
    return mconn.broadcastOperation(of, nodes);
  }

  private static void onReceivedStatus(OperationFuture<CASResponse> future, Operation op, OperationStatus val) {
    future.setCas(op.getResponseCas());
    future.set(new CASResponse(val.isSuccess(), op.getResponseCas()), val);
  }

  private static void onReceivedStatus(OperationFuture<CASResponse> future, Operation op, CASOperationStatus val) {
    future.setCas(op.getResponseCas());
    future.set(new CASResponse(val.getCASResponse(), op.getResponseCas()), val);
  }

  private static <T> void onComplete(MemcachedClientIF client,
                                 OperationFuture<T> future, Operation op, CountDownLatch latch,
                                 OperationListener<T> listener) {
    latch.countDown();
    if (listener != null) {
      OperationStatus status = future.getStatus();
      if (status == null) status = STATUS_ERROR;
      listener.onComplete(client, status, future);
    }
  }

  protected <T> OperationFuture<CASResponse> asyncStore(
      final MemcachedClientIF client, final MemcachedNode opNode, final StoreType storeType,
      final String key, final int exp, final T value, final Transcoder<T> tc,
      final OperationListener<CASResponse> listener) {
    final CachedData co = tc.encode(value);
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<CASResponse> rv = new OperationFuture<CASResponse>(key, latch, operationTimeout);
    final Operation op = opFact.store(storeType, key, co.getFlags(), exp, co.getData(),
      new OperationCallback() {
        @Override
        public void receivedStatus(Operation op, OperationStatus val) { onReceivedStatus(rv, op, val); }

        @Override
        public void complete(Operation op) { onComplete(client, rv, op, latch, listener); }
      });
    rv.setOperation(op);
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    return rv;
  }

  private OperationFuture<CASResponse> asyncStore(StoreType storeType, String key, int exp, Object value) {
    return asyncStore(this, null, storeType, key, exp, value, transcoder, null);
  }

  protected  <T> OperationFuture<CASResponse> asyncCat(
      final MemcachedClientIF client, final MemcachedNode opNode, final ConcatenationType catType,
      final long cas, final String key, final T value, final Transcoder<T> tc,
      final OperationListener<CASResponse> listener) {
    final CachedData co = tc.encode(value);
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<CASResponse> rv = new OperationFuture<CASResponse>(key, latch, operationTimeout);
    final Operation op = opFact.cat(catType, cas, key, co.getData(),
      new OperationCallback() {
        @Override
        public void receivedStatus(Operation op, OperationStatus val) { onReceivedStatus(rv, op, val); }

        @Override
        public void complete(Operation op) { onComplete(client, rv, op, latch, listener); }
      });
    rv.setOperation(op);
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    return rv;
  }

  /**
   * Touch the given key to reset its expiration time with the default
   * transcoder.
   *
   * @param key the key to fetch
   * @param exp the new expiration to set for the given key
   * @return a future that will hold the return value of whether or not the
   *         fetch succeeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> touch(final String key, final int exp) {
    return touch(key, exp, transcoder, null);
  }

  /**
   * Touch the given key to reset its expiration time with the default
   * transcoder.
   *
   * @param key the key to fetch
   * @param exp the new expiration to set for the given key
   * @return a future that will hold the return value of whether or not the
   *         fetch succeeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> Future<CASResponse> touch(String key, int exp, Transcoder<T> tc) {
    return touch(key, exp, transcoder, null);
  }

  /**
   * Touch the given key to reset its expiration time.
   *
   * @param key the key to fetch
   * @param exp the new expiration to set for the given key
   * @param tc the transcoder to serialize and unserialize value
   * @return a future that will hold the return value of whether or not the
   *         fetch succeeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> touch(final String key, final int exp,
      final Transcoder<T> tc, final OperationListener<CASResponse> listener) {
    return touch(this, null, key, exp, tc, listener);
  }

  protected <T> OperationFuture<CASResponse> touch(
      final MemcachedClientIF client, final MemcachedNode opNode,
      final String key, final int exp, final Transcoder<T> tc,
      final OperationListener<CASResponse> listener) {
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<CASResponse> rv = new OperationFuture<CASResponse>(key, latch, operationTimeout);
    final Operation op = opFact.touch(key, exp, new OperationCallback() {
      @Override
      public void receivedStatus(Operation op, OperationStatus val) { onReceivedStatus(rv, op, val); }

      @Override
      public void complete(Operation op) { onComplete(client, rv, op, latch, listener); }
    });
    rv.setOperation(op);
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    return rv;
  }

  /**
   * Append to an existing value in the cache.
   *
   * If 0 is passed in as the CAS identifier, it will override the value
   * on the server without performing the CAS check.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param cas cas identifier (ignored in the ascii protocol)
   * @param key the key to whose value will be appended
   * @param val the value to append
   * @return a future indicating success, false if there was no change to the
   *         value
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> append(long cas, String key, Object val) {
    return append(cas, key, val, transcoder, null);
  }

  /**
   * Append to an existing value in the cache.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param key the key to whose value will be appended
   * @param val the value to append
   * @return a future indicating success, false if there was no change to the
   *         value
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> append(String key, Object val) {
    return append(0, key, val, transcoder);
  }

  /**
   * Append to an existing value in the cache.
   *
   * If 0 is passed in as the CAS identifier, it will override the value
   * on the server without performing the CAS check.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param <T>
   * @param cas cas identifier (ignored in the ascii protocol)
   * @param key the key to whose value will be appended
   * @param val the value to append
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future indicating success
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> append(long cas, String key, T val, Transcoder<T> tc) {
    return asyncCat(this, null, ConcatenationType.append, cas, key, val, tc, null);
  }

  /**
   * Append to an existing value in the cache.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param <T>
   * @param cas cas identifier (ignored in the ascii protocol)
   * @param key the key to whose value will be appended
   * @param val the value to append
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future indicating success
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> append(long cas, String key, T val, Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return asyncCat(this, null, ConcatenationType.append, cas, key, val, tc, listener);
  }

  /**
   * Append to an existing value in the cache.
   *
   * If 0 is passed in as the CAS identifier, it will override the value
   * on the server without performing the CAS check.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param <T>
   * @param key the key to whose value will be appended
   * @param val the value to append
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future indicating success
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> append(String key, T val,
      Transcoder<T> tc) {
    return asyncCat(this, null, ConcatenationType.append, 0, key, val, tc, null);
  }

  /**
   * Prepend to an existing value in the cache.
   *
   * If 0 is passed in as the CAS identifier, it will override the value
   * on the server without performing the CAS check.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param cas cas identifier (ignored in the ascii protocol)
   * @param key the key to whose value will be prepended
   * @param val the value to append
   * @return a future indicating success
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> prepend(long cas, String key, Object val) {
    return prepend(cas, key, val, transcoder, null);
  }

  /**
   * Prepend to an existing value in the cache.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param key the key to whose value will be prepended
   * @param val the value to append
   * @return a future indicating success
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> prepend(String key, Object val) {
    return prepend(0, key, val, transcoder);
  }

  /**
   * Prepend to an existing value in the cache.
   *
   * If 0 is passed in as the CAS identifier, it will override the value
   * on the server without performing the CAS check.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param <T>
   * @param cas cas identifier (ignored in the ascii protocol)
   * @param key the key to whose value will be prepended
   * @param val the value to append
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future indicating success
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> prepend(long cas, String key, T val, Transcoder<T> tc) {
    return asyncCat(this, null, ConcatenationType.prepend, cas, key, val, tc, null);
  }

  /**
   * Prepend to an existing value in the cache.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param <T>
   * @param cas cas identifier (ignored in the ascii protocol)
   * @param key the key to whose value will be prepended
   * @param val the value to append
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future indicating success
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> prepend(long cas, String key, T val, Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return asyncCat(this, null, ConcatenationType.prepend, cas, key, val, tc, listener);
  }

  /**
   * Asynchronous CAS operation.
   *
   * @param <T>
   * @param key the key
   * @param casId the CAS identifier (from a gets operation)
   * @param value the new value
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future that will indicate the status of the CAS
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, T value, Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return asyncCAS(key, casId, 0, value, tc, listener);
  }

  /**
   * Prepend to an existing value in the cache.
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * @param <T>
   * @param key the key to whose value will be prepended
   * @param val the value to append
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future indicating success
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> prepend(String key, T val,
      Transcoder<T> tc) {
    return asyncCat(this, null, ConcatenationType.prepend, 0, key, val, tc, null);
  }

  /**
   * Asynchronous CAS operation.
   *
   * @param <T>
   * @param key the key
   * @param casId the CAS identifier (from a gets operation)
   * @param value the new value
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future that will indicate the status of the CAS
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, T value, Transcoder<T> tc) {
    return asyncCAS(key, casId, 0, value, tc, null);
  }


  /**
   * Asynchronous CAS operation.
   *
   * @param <T>
   * @param key the key
   * @param casId the CAS identifier (from a gets operation)
   * @param exp the expiration of this object
   * @param value the new value
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future that will indicate the status of the CAS
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, int exp,
      T value, Transcoder<T> tc, final OperationListener<CASResponse> listener) {
    return asyncCAS(this, null, key, casId, exp, value, tc, listener);
  }

  protected <T> OperationFuture<CASResponse> asyncCAS(
      final MemcachedClientIF client, final MemcachedNode opNode,
      final String key, final long casId, final int exp, final T value, final Transcoder<T> tc,
      final OperationListener<CASResponse> listener) {
    CachedData co = tc.encode(value);
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<CASResponse> rv =
            new OperationFuture<CASResponse>(key, latch, operationTimeout);
    final Operation op = opFact.cas(StoreType.set, key, casId, co.getFlags(), exp,
            co.getData(), new OperationCallback() {
      @Override
      public void receivedStatus(Operation op, OperationStatus val) {
        if (val instanceof CASOperationStatus) {
          onReceivedStatus(rv, op, (CASOperationStatus) val);
        } else {
          onReceivedStatus(rv, op, val);
        }
      }

      @Override
      public void complete(Operation op) { onComplete(client, rv, op, latch, listener); }
    });
    rv.setOperation(op);
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    return rv;
  }


  /**
   * Asynchronous CAS operation.
   *
   * @param <T>
   * @param key the key
   * @param casId the CAS identifier (from a gets operation)
   * @param exp the expiration of this object
   * @param value the new value
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future that will indicate the status of the CAS
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public <T> OperationFuture<CASResponse> asyncCAS(String key, long casId, int exp,
      T value, Transcoder<T> tc) {
    return asyncCAS(key, casId, exp, value, tc, null);
  }

  /**
   * Asynchronous CAS operation using the default transcoder.
   *
   * @param key the key
   * @param casId the CAS identifier (from a gets operation)
   * @param value the new value
   * @return a future that will indicate the status of the CAS
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> asyncCAS(String key, long casId, Object value) {
    return asyncCAS(key, casId, value, transcoder);
  }

  /**
   * Perform a synchronous CAS operation.
   *
   * @param <T>
   * @param key the key
   * @param casId the CAS identifier (from a gets operation)
   * @param value the new value
   * @param tc the transcoder to serialize and unserialize the value
   * @return a CASResponse
   * @throws OperationTimeoutException if global operation timeout is exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  public <T> CASResponse cas(String key, long casId, T value,
      Transcoder<T> tc) {
    return cas(key, casId, 0, value, tc);
  }

  /**
   * Perform a synchronous CAS operation.
   *
   * @param <T>
   * @param key the key
   * @param casId the CAS identifier (from a gets operation)
   * @param exp the expiration of this object
   * @param value the new value
   * @param tc the transcoder to serialize and unserialize the value
   * @return a CASResponse
   * @throws OperationTimeoutException if global operation timeout is exceeded
   * @throws CancellationException if operation was canceled
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> CASResponse cas(String key, long casId, int exp, T value,
      Transcoder<T> tc) {
    try {
      return asyncCAS(key, casId, exp, value, tc).get(operationTimeout,
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof CancellationException) {
        throw (CancellationException) e.getCause();
      } else {
        throw new RuntimeException("Exception waiting for value", e);
      }
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value: "
        + buildTimeoutMessage(operationTimeout, TimeUnit.MILLISECONDS), e);
    }
  }

  /**
   * Perform a synchronous CAS operation with the default transcoder.
   *
   * @param key the key
   * @param casId the CAS identifier (from a gets operation)
   * @param value the new value
   * @return a CASResponse
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public CASResponse cas(String key, long casId, Object value) {
    return cas(key, casId, value, transcoder);
  }

  /**
   * Perform a synchronous CAS operation with the default transcoder.
   *
   * @param key the key
   * @param casId the CAS identifier (from a gets operation)
   * @param exp the expiration of this object
   * @param value the new value
   * @return a CASResponse
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public CASResponse cas(String key, long casId, int exp, Object value) {
    return cas(key, casId, exp, value, transcoder);
  }

  /**
   * Add an object to the cache iff it does not exist already.
   *
   * <p>
   * The <code>exp</code> value is passed along to memcached exactly as given,
   * and will be processed per the memcached protocol specification:
   * </p>
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * <blockquote>
   * <p>
   * The actual value sent may either be Unix time (number of seconds since
   * January 1, 1970, as a 32-bit value), or a number of seconds starting from
   * current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client
   * is larger than that, the server will consider it to be real Unix time value
   * rather than an offset from current time.
   * </p>
   * </blockquote>
   *
   * @param <T>
   * @param key the key under which this object should be added.
   * @param exp the expiration of this object
   * @param o the object to store
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future representing the processing of this operation
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> add(String key, int exp, T o,
      Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return asyncStore(this, null, StoreType.add, key, exp, o, tc, listener);
  }

  /**
   * Add an object to the cache iff it does not exist already.
   *
   * <p>
   * The <code>exp</code> value is passed along to memcached exactly as given,
   * and will be processed per the memcached protocol specification:
   * </p>
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * <blockquote>
   * <p>
   * The actual value sent may either be Unix time (number of seconds since
   * January 1, 1970, as a 32-bit value), or a number of seconds starting from
   * current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client
   * is larger than that, the server will consider it to be real Unix time value
   * rather than an offset from current time.
   * </p>
   * </blockquote>
   *
   * @param <T>
   * @param key the key under which this object should be added.
   * @param exp the expiration of this object
   * @param o the object to store
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future representing the processing of this operation
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> add(String key, int exp, T o,
      Transcoder<T> tc) {
    return asyncStore(this, null, StoreType.add, key, exp, o, tc, null);
  }

  /**
   * Add an object to the cache (using the default transcoder) iff it does not
   * exist already.
   *
   * <p>
   * The <code>exp</code> value is passed along to memcached exactly as given,
   * and will be processed per the memcached protocol specification:
   * </p>
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * <blockquote>
   * <p>
   * The actual value sent may either be Unix time (number of seconds since
   * January 1, 1970, as a 32-bit value), or a number of seconds starting from
   * current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client
   * is larger than that, the server will consider it to be real Unix time value
   * rather than an offset from current time.
   * </p>
   * </blockquote>
   *
   * @param key the key under which this object should be added.
   * @param exp the expiration of this object
   * @param o the object to store
   * @return a future representing the processing of this operation
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> add(String key, int exp, Object o) {
    return asyncStore(this, null, StoreType.add, key, exp, o, transcoder, null);
  }

  /**
   * Set an object in the cache regardless of any existing value.
   *
   * <p>
   * The <code>exp</code> value is passed along to memcached exactly as given,
   * and will be processed per the memcached protocol specification:
   * </p>
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * <blockquote>
   * <p>
   * The actual value sent may either be Unix time (number of seconds since
   * January 1, 1970, as a 32-bit value), or a number of seconds starting from
   * current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client
   * is larger than that, the server will consider it to be real Unix time value
   * rather than an offset from current time.
   * </p>
   * </blockquote>
   *
   * @param <T>
   * @param key the key under which this object should be added.
   * @param exp the expiration of this object
   * @param o the object to store
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future representing the processing of this operation
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> set(String key, int exp, T o,
      Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return asyncStore(this, null, StoreType.set, key, exp, o, tc, listener);
  }

  /**
   * Set an object in the cache regardless of any existing value.
   *
   * <p>
   * The <code>exp</code> value is passed along to memcached exactly as given,
   * and will be processed per the memcached protocol specification:
   * </p>
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * <blockquote>
   * <p>
   * The actual value sent may either be Unix time (number of seconds since
   * January 1, 1970, as a 32-bit value), or a number of seconds starting from
   * current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client
   * is larger than that, the server will consider it to be real Unix time value
   * rather than an offset from current time.
   * </p>
   * </blockquote>
   *
   * @param <T>
   * @param key the key under which this object should be added.
   * @param exp the expiration of this object
   * @param o the object to store
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future representing the processing of this operation
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> set(String key, int exp, T o,
      Transcoder<T> tc) {
    return asyncStore(this, null, StoreType.set, key, exp, o, tc, null);
  }

  /**
   * Set an object in the cache (using the default transcoder) regardless of any
   * existing value.
   *
   * <p>
   * The <code>exp</code> value is passed along to memcached exactly as given,
   * and will be processed per the memcached protocol specification:
   * </p>
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * <blockquote>
   * <p>
   * The actual value sent may either be Unix time (number of seconds since
   * January 1, 1970, as a 32-bit value), or a number of seconds starting from
   * current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client
   * is larger than that, the server will consider it to be real Unix time value
   * rather than an offset from current time.
   * </p>
   * </blockquote>
   *
   * @param key the key under which this object should be added.
   * @param exp the expiration of this object
   * @param o the object to store
   * @return a future representing the processing of this operation
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> set(String key, int exp, Object o) {
    return asyncStore(this, null, StoreType.set, key, exp, o, transcoder, null);
  }

  /**
   * Replace an object with the given value iff there is already a value for the
   * given key.
   *
   * <p>
   * The <code>exp</code> value is passed along to memcached exactly as given,
   * and will be processed per the memcached protocol specification:
   * </p>
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * <blockquote>
   * <p>
   * The actual value sent may either be Unix time (number of seconds since
   * January 1, 1970, as a 32-bit value), or a number of seconds starting from
   * current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client
   * is larger than that, the server will consider it to be real Unix time value
   * rather than an offset from current time.
   * </p>
   * </blockquote>
   *
   * @param <T>
   * @param key the key under which this object should be added.
   * @param exp the expiration of this object
   * @param o the object to store
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future representing the processing of this operation
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> replace(String key, int exp, T o,
      Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return asyncStore(this, null, StoreType.replace, key, exp, o, tc, listener);
  }

  /**
   * Replace an object with the given value iff there is already a value for the
   * given key.
   *
   * <p>
   * The <code>exp</code> value is passed along to memcached exactly as given,
   * and will be processed per the memcached protocol specification:
   * </p>
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * <blockquote>
   * <p>
   * The actual value sent may either be Unix time (number of seconds since
   * January 1, 1970, as a 32-bit value), or a number of seconds starting from
   * current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client
   * is larger than that, the server will consider it to be real Unix time value
   * rather than an offset from current time.
   * </p>
   * </blockquote>
   *
   * @param <T>
   * @param key the key under which this object should be added.
   * @param exp the expiration of this object
   * @param o the object to store
   * @param tc the transcoder to serialize and unserialize the value
   * @return a future representing the processing of this operation
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASResponse> replace(String key, int exp, T o,
      Transcoder<T> tc) {
    return asyncStore(this, null, StoreType.replace, key, exp, o, tc, null);
  }

  /**
   * Replace an object with the given value (transcoded with the default
   * transcoder) iff there is already a value for the given key.
   *
   * <p>
   * The <code>exp</code> value is passed along to memcached exactly as given,
   * and will be processed per the memcached protocol specification:
   * </p>
   *
   * <p>
   * Note that the return will be false any time a mutation has not occurred.
   * </p>
   *
   * <blockquote>
   * <p>
   * The actual value sent may either be Unix time (number of seconds since
   * January 1, 1970, as a 32-bit value), or a number of seconds starting from
   * current time. In the latter case, this number of seconds may not exceed
   * 60*60*24*30 (number of seconds in 30 days); if the number sent by a client
   * is larger than that, the server will consider it to be real Unix time value
   * rather than an offset from current time.
   * </p>
   * </blockquote>
   *
   * @param key the key under which this object should be added.
   * @param exp the expiration of this object
   * @param o the object to store
   * @return a future representing the processing of this operation
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> replace(String key, int exp, Object o) {
    return asyncStore(this, null, StoreType.replace, key, exp, o, transcoder, null);
  }

  /**
   * Get the given key asynchronously.
   *
   * @param <T>
   * @param key the key to fetch
   * @param tc the transcoder to serialize and unserialize value
   * @param listener the listener to handle complete event
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> GetFuture<T> asyncGet(final String key, final Transcoder<T> tc, final OperationListener<T> listener) {
    return asyncGet(this, null, key, tc, listener);
  }

  protected <T> GetFuture<T> asyncGet(
      final MemcachedClientIF client, final MemcachedNode opNode,
      final String key, final Transcoder<T> tc,
      final OperationListener<T> listener) {
    final CountDownLatch latch = new CountDownLatch(1);
    final GetFuture<T> rv = new GetFuture<T>(latch, operationTimeout, key);
    final Operation op = opFact.get(key, new DataCallback() {
      private Future<T> val = null;

      @Override
      public void gotData(String k, int flags, long cas, byte[] data) {
        assert key.equals(k) : "Wrong key returned";
        val = tcService.decode(tc, new CachedData(flags, data, tc.getMaxSize()));
      }

      @Override
      public void receivedStatus(Operation op, OperationStatus status) { rv.set(val, status); }

      @Override
      public void complete(Operation op) {
        latch.countDown();
        if (listener != null) {
          OperationStatus status = rv.getStatus();
          if (status == null) status = STATUS_ERROR;
          listener.onComplete(client, status, rv);
        }
      }
    });
    rv.setOperation(op);
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    return rv;
  }

  /**
   * Get the given key asynchronously.
   *
   * @param <T>
   * @param key the key to fetch
   * @param tc the transcoder to serialize and unserialize value
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> GetFuture<T> asyncGet(final String key, final Transcoder<T> tc) {
    return asyncGet(key, tc, null);
  }

  /**
   * Get the given key asynchronously and decode with the default transcoder.
   *
   * @param key the key to fetch
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public GetFuture<Object> asyncGet(final String key) {
    return asyncGet(key, transcoder);
  }

  /**
   * Gets (with CAS support) the given key asynchronously.
   *
   * @param <T>
   * @param key the key to fetch
   * @param tc the transcoder to serialize and unserialize value
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASValue<T>> asyncGets(final String key,
      final Transcoder<T> tc, final OperationListener<CASValue<T>> listener) {
    return asyncGets(this, null, key, tc, listener);
  }

  protected <T> OperationFuture<CASValue<T>> asyncGets(
      final MemcachedClientIF client, final MemcachedNode opNode,
      final String key, final Transcoder<T> tc,
      final OperationListener<CASValue<T>> listener) {
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<CASValue<T>> rv = new OperationFuture<CASValue<T>>(key, latch, operationTimeout);
    final Operation op = opFact.gets(key, new DataCallback() {
      private CASValue<T> val = null;

      @Override
      public void gotData(String k, int flags, long cas, byte[] data) {
        assert key.equals(k) : "Wrong key returned";
        assert cas > 0 : "CAS was less than zero:  " + cas;
        val = new CASValue<T>(cas, tc.decode(new CachedData(flags, data, tc.getMaxSize())));
      }

      @Override
      public void receivedStatus(Operation op, OperationStatus status) { rv.set(val, status); }

      @Override
      public void complete(Operation op) { onComplete(client, rv, op, latch, listener); }
    });
    rv.setOperation(op);
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    return rv;
  }

  /**
   * Gets (with CAS support) the given key asynchronously.
   *
   * @param <T>
   * @param key the key to fetch
   * @param tc the transcoder to serialize and unserialize value
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASValue<T>> asyncGets(final String key,
      final Transcoder<T> tc) {
    return asyncGets(key, tc, null);
  }

  /**
   * Gets (with CAS support) the given key asynchronously and decode using the
   * default transcoder.
   *
   * @param key the key to fetch
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASValue<Object>> asyncGets(final String key) {
    return asyncGets(key, transcoder);
  }

  /**
   * Gets (with CAS support) with a single key.
   *
   * @param <T>
   * @param key the key to get
   * @param tc the transcoder to serialize and unserialize value
   * @return the result from the cache and CAS id (null if there is none)
   * @throws OperationTimeoutException if global operation timeout is exceeded
   * @throws CancellationException if operation was canceled
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> CASValue<T> gets(String key, Transcoder<T> tc) {
    try {
      return asyncGets(key, tc).get(operationTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof CancellationException) {
        throw (CancellationException) e.getCause();
      } else {
        throw new RuntimeException("Exception waiting for value", e);
      }
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value", e);
    }
  }

  /**
   * Get with a single key and reset its expiration.
   *
   * @param <T>
   * @param key the key to get
   * @param exp the new expiration for the key
   * @param tc the transcoder to serialize and unserialize value
   * @return the result from the cache (null if there is none)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws CancellationException if operation was canceled
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> CASValue<T> getAndTouch(String key, int exp, Transcoder<T> tc) {
    try {
      return asyncGetAndTouch(key, exp, tc).get(operationTimeout,
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof CancellationException) {
        throw (CancellationException) e.getCause();
      } else {
        throw new RuntimeException("Exception waiting for value", e);
      }
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value", e);
    }
  }

  /**
   * Get a single key and reset its expiration using the default transcoder.
   *
   * @param key the key to get
   * @param exp the new expiration for the key
   * @return the result from the cache and CAS id (null if there is none)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public CASValue<Object> getAndTouch(String key, int exp) {
    return getAndTouch(key, exp, transcoder);
  }

  /**
   * Gets (with CAS support) with a single key using the default transcoder.
   *
   * @param key the key to get
   * @return the result from the cache and CAS id (null if there is none)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public CASValue<Object> gets(String key) {
    return gets(key, transcoder);
  }

  /**
   * Get with a single key.
   *
   * @param <T>
   * @param key the key to get
   * @param tc the transcoder to serialize and unserialize value
   * @return the result from the cache (null if there is none)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws CancellationException if operation was canceled
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> T get(String key, Transcoder<T> tc) {
    try {
      return asyncGet(key, tc).get(operationTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof CancellationException) {
        throw (CancellationException) e.getCause();
      } else {
        throw new RuntimeException("Exception waiting for value", e);
      }
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value: "
        + buildTimeoutMessage(operationTimeout, TimeUnit.MILLISECONDS), e);
    }
  }

  /**
   * Get with a single key and decode using the default transcoder.
   *
   * @param key the key to get
   * @return the result from the cache (null if there is none)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public Object get(String key) {
    return get(key, transcoder);
  }

  private <T> Map<MemcachedNode, Collection<String>> getChunks(Iterator<String> keyIter,
                                                               Iterator<Transcoder<T>> tcIter,
                                                               Map<String, Transcoder<T>> outTcMap) {
    final Map<MemcachedNode, Collection<String>> chunks = new HashMap<MemcachedNode, Collection<String>>();
    final NodeLocator locator = mconn.getLocator();

    while (keyIter.hasNext() && tcIter.hasNext()) {
      String key = keyIter.next();
      outTcMap.put(key, tcIter.next());
      StringUtils.validateKey(key, opFact instanceof BinaryOperationFactory);
      final MemcachedNode primaryNode = locator.getPrimary(key);
      MemcachedNode node = null;
      if (primaryNode.isActive()) {
        node = primaryNode;
      } else {
        for (Iterator<MemcachedNode> i = locator.getSequence(key); node == null
                && i.hasNext();) {
          MemcachedNode n = i.next();
          if (n.isActive()) {
            node = n;
          }
        }
        if (node == null) {
          node = primaryNode;
        }
      }
      assert node != null : "Didn't find a node for " + key;
      Collection<String> ks = chunks.get(node);
      if (ks == null) {
        ks = new ArrayList<String>();
        chunks.put(node, ks);
      }
      ks.add(key);
    }

    return chunks;
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keyIter Iterator that produces keys.
   * @param tcIter an iterator of transcoders to serialize and unserialize
   *          values; the transcoders are matched with the keys in the same
   *          order. The minimum of the key collection length and number of
   *          transcoders is used and no exception is thrown if they do not
   *          match
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
      Iterator<Transcoder<T>> tcIter, final OperationListener<Map<String, T>> listener) {
    final Map<String, Future<T>> m = new ConcurrentHashMap<String, Future<T>>();

    // This map does not need to be a ConcurrentHashMap
    // because it is fully populated when it is used and
    // used only to read the transcoder for a key.
    final Map<String, Transcoder<T>> tcMap = new HashMap<String, Transcoder<T>>();

    // Break the gets down into groups by key
    final Map<MemcachedNode, Collection<String>> chunks = getChunks(keyIter, tcIter, tcMap);

    final MemcachedClient client = this;
    final CountDownLatch latch = new CountDownLatch(chunks.size());
    final Collection<Operation> ops = new ArrayList<Operation>(chunks.size());
    final BulkGetFuture<T> rv = new BulkGetFuture<T>(m, ops, latch);

    final DataCallback cb = new DataCallback() {
      @Override
      public void gotData(String k, int flags, long cas, byte[] data) {
        Transcoder<T> tc = tcMap.get(k);
        m.put(k, tcService.decode(tc, new CachedData(flags, data, tc.getMaxSize())));
      }

      @Override
      @SuppressWarnings("synthetic-access")
      public void receivedStatus(Operation op, OperationStatus status) { rv.setStatus(status); }

      @Override
      public void complete(Operation op) {
        latch.countDown();
        if (listener != null && latch.getCount() < 1) {
          OperationStatus st = rv.getStatus();
          if (st == null) st = STATUS_ERROR;
          listener.onComplete(client, st, rv);
        }
      }
    };

    // Now that we know how many servers it breaks down into, and the latch
    // is all set up, convert all of these strings collections to operations
    final Map<MemcachedNode, Operation> mops =
        new HashMap<MemcachedNode, Operation>();

    for (Map.Entry<MemcachedNode, Collection<String>> me : chunks.entrySet()) {
      Operation op = opFact.get(me.getValue(), cb);
      mops.put(me.getKey(), op);
      ops.add(op);
    }
    assert mops.size() == chunks.size();
    mconn.checkState();
    mconn.addOperations(mops);
    return rv;
  }

  /**
   * Asynchronously get a bunch of CASed objects from the cache.
   *
   * @param <T>
   * @param keyIter Iterator that produces keys.
   * @param tcIter an iterator of transcoders to serialize and unserialize
   *          values; the transcoders are matched with the keys in the same
   *          order. The minimum of the key collection length and number of
   *          transcoders is used and no exception is thrown if they do not
   *          match
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter,
       Iterator<Transcoder<T>> tcIter, final OperationListener<Map<String, CASValue<T>>> listener) {
    final Map<String, Future<CASValue<T>>> m = new ConcurrentHashMap<String, Future<CASValue<T>>>();

    // This map does not need to be a ConcurrentHashMap
    // because it is fully populated when it is used and
    // used only to read the transcoder for a key.
    final Map<String, Transcoder<T>> tcMap = new HashMap<String, Transcoder<T>>();

    // Break the gets down into groups by key
    final Map<MemcachedNode, Collection<String>> chunks = getChunks(keyIter, tcIter, tcMap);

    final MemcachedClient client = this;
    final CountDownLatch latch = new CountDownLatch(chunks.size());
    final Collection<Operation> ops = new ArrayList<Operation>(chunks.size());
    final BulkGetFuture<CASValue<T>> rv = new BulkGetFuture<CASValue<T>>(m, ops, latch);

    final DataCallback cb = new DataCallback() {

      @Override
      public void gotData(String k, int flags, long cas, byte[] data) {
        assert cas > 0 : "CAS was less than zero:  " + cas;
        Transcoder<T> tc = tcMap.get(k);
        m.put(k, tcService.decodes(tc, new CachedData(flags, data, tc.getMaxSize()), cas));
      }

      @Override
      @SuppressWarnings("synthetic-access")
      public void receivedStatus(Operation op, OperationStatus status) { rv.setStatus(status); }

      @Override
      public void complete(Operation op) {
        latch.countDown();
        if (listener != null && latch.getCount() < 1) {
          OperationStatus st = rv.getStatus();
          if (st == null) st = STATUS_ERROR;
          listener.onComplete(client, st, rv);
        }
      }
    };

    // Now that we know how many servers it breaks down into, and the latch
    // is all set up, convert all of these strings collections to operations
    final Map<MemcachedNode, Operation> mops =
            new HashMap<MemcachedNode, Operation>();

    for (Map.Entry<MemcachedNode, Collection<String>> me : chunks.entrySet()) {
      Operation op = opFact.gets(me.getValue(), cb);
      mops.put(me.getKey(), op);
      ops.add(op);
    }
    assert mops.size() == chunks.size();
    mconn.checkState();
    mconn.addOperations(mops);
    return rv;
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keyIter Iterator that produces keys.
   * @param tcIter an iterator of transcoders to serialize and unserialize
   *          values; the transcoders are matched with the keys in the same
   *          order. The minimum of the key collection length and number of
   *          transcoders is used and no exception is thrown if they do not
   *          match
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter,
      Iterator<Transcoder<T>> tcIter) {
    return asyncGetsBulk(keyIter, tcIter, null);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keys the keys to request
   * @param tcIter an iterator of transcoders to serialize and unserialize
   *          values; the transcoders are matched with the keys in the same
   *          order. The minimum of the key collection length and number of
   *          transcoders is used and no exception is thrown if they do not
   *          match
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
      Iterator<Transcoder<T>> tcIter, OperationListener<Map<String, CASValue<T>>> listener) {
    return asyncGetsBulk(keys.iterator(), tcIter, listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keys the keys to request
   * @param tcIter an iterator of transcoders to serialize and unserialize
   *          values; the transcoders are matched with the keys in the same
   *          order. The minimum of the key collection length and number of
   *          transcoders is used and no exception is thrown if they do not
   *          match
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
      Iterator<Transcoder<T>> tcIter) {
    return asyncGetsBulk(keys.iterator(), tcIter, null);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keyIter Iterator for the keys to request
   * @param tc the transcoder to serialize and unserialize values
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter,
      Transcoder<T> tc, OperationListener<Map<String, CASValue<T>>> listener) {
    return asyncGetsBulk(keyIter, new SingleElementInfiniteIterator<Transcoder<T>>(tc), listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keyIter Iterator for the keys to request
   * @param tc the transcoder to serialize and unserialize values
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter,
      Transcoder<T> tc) {
    return asyncGetsBulk(keyIter,
            new SingleElementInfiniteIterator<Transcoder<T>>(tc));
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keys the keys to request
   * @param tc the transcoder to serialize and unserialize values
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
      Transcoder<T> tc, OperationListener<Map<String, CASValue<T>>> listener) {
    return asyncGetsBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keys the keys to request
   * @param tc the transcoder to serialize and unserialize values
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
     Transcoder<T> tc) {
    return asyncGetsBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), null);
  }

  /**
   * Asynchronously get a bunch of objects from the cache and decode them with
   * the given transcoder.
   *
   * @param keyIter Iterator that produces the keys to request
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(
          Iterator<String> keyIter, OperationListener<Map<String, CASValue<Object>>> listener) {
    return asyncGetsBulk(keyIter, transcoder, listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache and decode them with
   * the given transcoder.
   *
   * @param keyIter Iterator that produces the keys to request
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(
          Iterator<String> keyIter) {
    return asyncGetsBulk(keyIter, transcoder);
  }

  /**
   * Asynchronously get a bunch of objects from the cache and decode them with
   * the given transcoder.
   *
   * @param keys the keys to request
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(Collection<String> keys, OperationListener<Map<String, CASValue<Object>>> listener) {
    return asyncGetsBulk(keys, transcoder, listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache and decode them with
   * the given transcoder.
   *
   * @param keys the keys to request
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(Collection<String> keys) {
    return asyncGetsBulk(keys, transcoder);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keyIter Iterator that produces keys.
   * @param tcIter an iterator of transcoders to serialize and unserialize
   *          values; the transcoders are matched with the keys in the same
   *          order. The minimum of the key collection length and number of
   *          transcoders is used and no exception is thrown if they do not
   *          match
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
      Iterator<Transcoder<T>> tcIter) {
    return asyncGetBulk(keyIter, tcIter, null); 
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keys the keys to request
   * @param tcIter an iterator of transcoders to serialize and unserialize
   *          values; the transcoders are matched with the keys in the same
   *          order. The minimum of the key collection length and number of
   *          transcoders is used and no exception is thrown if they do not
   *          match
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
          Iterator<Transcoder<T>> tcIter, OperationListener<Map<String, T>> listener) {
    return asyncGetBulk(keys.iterator(), tcIter, listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keys the keys to request
   * @param tcIter an iterator of transcoders to serialize and unserialize
   *          values; the transcoders are matched with the keys in the same
   *          order. The minimum of the key collection length and number of
   *          transcoders is used and no exception is thrown if they do not
   *          match
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
          Iterator<Transcoder<T>> tcIter) {
    return asyncGetBulk(keys.iterator(), tcIter, null);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keyIter Iterator for the keys to request
   * @param tc the transcoder to serialize and unserialize values
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
      Transcoder<T> tc, OperationListener<Map<String, T>> listener) {
    return asyncGetBulk(keyIter, new SingleElementInfiniteIterator<Transcoder<T>>(tc), listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keyIter Iterator for the keys to request
   * @param tc the transcoder to serialize and unserialize values
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
      Transcoder<T> tc) {
    return asyncGetBulk(keyIter,
            new SingleElementInfiniteIterator<Transcoder<T>>(tc));
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keys the keys to request
   * @param tc the transcoder to serialize and unserialize values
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
      Transcoder<T> tc, OperationListener<Map<String, T>> listener) {
    return asyncGetBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
        tc), listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache.
   *
   * @param <T>
   * @param keys the keys to request
   * @param tc the transcoder to serialize and unserialize values
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
      Transcoder<T> tc) {
    return asyncGetBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), null);
  }

  /**
   * Asynchronously get a bunch of objects from the cache and decode them with
   * the given transcoder.
   *
   * @param keyIter Iterator that produces the keys to request
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(
         Iterator<String> keyIter, OperationListener<Map<String, Object>> listener) {
    return asyncGetBulk(keyIter, transcoder, listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache and decode them with
   * the given transcoder.
   *
   * @param keyIter Iterator that produces the keys to request
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(
         Iterator<String> keyIter) {
    return asyncGetBulk(keyIter, transcoder);
  }

  /**
   * Asynchronously get a bunch of objects from the cache and decode them with
   * the given transcoder.
   *
   * @param keys the keys to request
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys, OperationListener<Map<String, Object>> listener) {
    return asyncGetBulk(keys, transcoder, listener);
  }

  /**
   * Asynchronously get a bunch of objects from the cache and decode them with
   * the given transcoder.
   *
   * @param keys the keys to request
   * @return a Future result of that fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
    return asyncGetBulk(keys, transcoder);
  }

  /**
   * Varargs wrapper for asynchronous bulk gets.
   *
   * @param <T>
   * @param tc the transcoder to serialize and unserialize value
   * @param keys one more more keys to get
   * @return the future values of those keys
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(OperationListener<Map<String, T>> listener, Transcoder<T> tc, String... keys) {
    return asyncGetBulk(Arrays.asList(keys), tc, listener);
  }

  /**
   * Varargs wrapper for asynchronous bulk gets.
   *
   * @param <T>
   * @param tc the transcoder to serialize and unserialize value
   * @param keys one more more keys to get
   * @return the future values of those keys
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> tc,
      String... keys) {
    return asyncGetBulk(Arrays.asList(keys), tc);
  }

  /**
   * Varargs wrapper for asynchronous bulk gets with the default transcoder.
   *
   * @param keys one more more keys to get
   * @return the future values of those keys
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(OperationListener<Map<String, Object>> listener, String... keys) {
    return asyncGetBulk(Arrays.asList(keys), transcoder, listener);
  }

  /**
   * Varargs wrapper for asynchronous bulk gets with the default transcoder.
   *
   * @param keys one more more keys to get
   * @return the future values of those keys
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys) {
    return asyncGetBulk(Arrays.asList(keys), transcoder);
  }

  /**
   * Get the given key to reset its expiration time.
   *
   * @param key the key to fetch
   * @param exp the new expiration to set for the given key
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASValue<Object>> asyncGetAndTouch(final String key,
      final int exp) {
    return asyncGetAndTouch(key, exp, transcoder);
  }

  /**
   * Get the given key to reset its expiration time.
   *
   * @param key the key to fetch
   * @param exp the new expiration to set for the given key
   * @param tc the transcoder to serialize and unserialize value
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASValue<T>> asyncGetAndTouch(final String key,
     final int exp, final Transcoder<T> tc) {
    return asyncGetAndTouch(key, exp, tc, null);
  }

  /**
   * Get the given key to reset its expiration time.
   *
   * @param key the key to fetch
   * @param exp the new expiration to set for the given key
   * @param tc the transcoder to serialize and unserialize value
   * @return a future that will hold the return value of the fetch
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> OperationFuture<CASValue<T>> asyncGetAndTouch(final String key,
      final int exp, final Transcoder<T> tc, final OperationListener<CASValue<T>> listener) {
    return asyncGetAndTouch(this, null, key, exp, tc, listener);
  }

  protected <T> OperationFuture<CASValue<T>> asyncGetAndTouch(
      final MemcachedClientIF client, final MemcachedNode opNode,
      final String key, final int exp, final Transcoder<T> tc,
      final OperationListener<CASValue<T>> listener) {
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<CASValue<T>> rv = new OperationFuture<CASValue<T>>(
            key, latch, operationTimeout);

    final Operation op = opFact.getAndTouch(key, exp, new DataCallback() {
      private CASValue<T> val = null;

      @Override
      public void gotData(String k, int flags, long cas, byte[] data) {
        assert k.equals(key) : "Wrong key returned";
        assert cas > 0 : "CAS was less than zero:  " + cas;
        val = new CASValue<T>(cas, tc.decode(new CachedData(flags, data, tc.getMaxSize())));
      }

      @Override
      public void receivedStatus(Operation op, OperationStatus status) { rv.set(val, status); }

      @Override
      public void complete(Operation op) { onComplete(client, rv, op, latch, listener); }
    });
    rv.setOperation(op);
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    return rv;
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param <T>
   * @param keyIter Iterator that produces the keys
   * @param tc the transcoder to serialize and unserialize value
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws CancellationException if operation was canceled
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> Map<String, T> getBulk(Iterator<String> keyIter,
      Transcoder<T> tc) {
    try {
      return asyncGetBulk(keyIter, tc).get(operationTimeout,
          TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted getting bulk values", e);
    } catch (ExecutionException e) {
      if(e.getCause() instanceof CancellationException) {
        throw (CancellationException) e.getCause();
      } else {
        throw new RuntimeException("Exception waiting for bulk values", e);
      }
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for bulk values: "
        + buildTimeoutMessage(operationTimeout, TimeUnit.MILLISECONDS), e);
    }
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param keyIter Iterator that produces the keys
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public Map<String, Object> getBulk(Iterator<String> keyIter) {
    return getBulk(keyIter, transcoder);
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param <T>
   * @param keys the keys
   * @param tc the transcoder to serialize and unserialize value
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> Map<String, T> getBulk(Collection<String> keys,
      Transcoder<T> tc) {
    return getBulk(keys.iterator(), tc);
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param keys the keys
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public Map<String, Object> getBulk(Collection<String> keys) {
    return getBulk(keys, transcoder);
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param <T>
   * @param tc the transcoder to serialize and unserialize value
   * @param keys the keys
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) {
    return getBulk(Arrays.asList(keys), tc);
  }

  /**
   * Get the values for multiple keys from the cache.
   *
   * @param keys the keys
   * @return a map of the values (for each value that exists)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public Map<String, Object> getBulk(String... keys) {
    return getBulk(Arrays.asList(keys), transcoder);
  }

  /**
   * Get the versions of all of the connected memcacheds.
   *
   * @return a Map of SocketAddress to String for connected servers
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public Map<SocketAddress, String> getVersions() {
    final Map<SocketAddress, String> rv =
        new ConcurrentHashMap<SocketAddress, String>();

    final CountDownLatch blatch = broadcastOp(new BroadcastOpFactory() {
      public Operation newOp(final MemcachedNode n,
          final CountDownLatch latch) {
        final SocketAddress sa = n.getSocketAddress();
        return opFact.version(new OperationCallback() {

          @Override
          public void receivedStatus(Operation op, OperationStatus s) {
            rv.put(sa, s.getMessage());
          }

          @Override
          public void complete(Operation op) {
            latch.countDown();
          }
        });
      }
    });
    try {
      blatch.await(operationTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for versions", e);
    }
    return rv;
  }

  /**
   * Get all of the local stats from all of the connections. This will not 
   * send a call to server but report only stats that are known to client.
   * The stats are grouped by Node (SocketAddress) and Stat ID
   *
   * @return a Map of a Map of stats by Node
   * 
   */
  public List<LocalStatNode> getLocalStats() {
    return mconn.getLocalStats();
  }
  
  /**
   * Get all of the stats from all of the connections.
   *
   * @return a Map of a Map of stats replies by SocketAddress
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public Map<SocketAddress, Map<String, String>> getStats() {
    return getStats(null);
  }

  /**
   * Get a set of stats from all connections.
   *
   * @param arg which stats to get
   * @return a Map of the server SocketAddress to a map of String stat keys to
   *         String stat values.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public Map<SocketAddress, Map<String, String>> getStats(final String arg) {
    final Map<SocketAddress, Map<String, String>> rv =
        new HashMap<SocketAddress, Map<String, String>>();

    final CountDownLatch blatch = broadcastOp(new BroadcastOpFactory() {
      public Operation newOp(final MemcachedNode n,
          final CountDownLatch latch) {
        final SocketAddress sa = n.getSocketAddress();
        rv.put(sa, new HashMap<String, String>());
        return opFact.stats(arg, new StatsOperation.Callback() {

          @Override
          public void gotStat(String name, String val) {
            rv.get(sa).put(name, val);
          }

          @Override
          @SuppressWarnings("synthetic-access")
          public void receivedStatus(Operation op, OperationStatus status) {
            if (!status.isSuccess()) {
              getLogger().warn("Unsuccessful stat fetch: %s", status);
            }
          }

          @Override
          public void complete(Operation op) {
            latch.countDown();
          }
        });
      }
    });
    try {
      blatch.await(operationTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for stats", e);
    }
    return rv;
  }

  protected long mutate(MemcachedNode opNode, Mutator m, String key, long by, long def, int exp) {
    final AtomicLong rv = new AtomicLong();
    final CountDownLatch latch = new CountDownLatch(1);
    final Operation op = opFact.mutate(m, key, by, def, exp,
        new OperationCallback() {
          @Override
          public void receivedStatus(Operation op, OperationStatus s) {
            // XXX: Potential abstraction leak.
            // The handling of incr/decr in the binary protocol
            // Allows us to avoid string processing.
            rv.set(new Long(s.isSuccess() ? s.getMessage() : "-1"));
          }

          @Override
          public void complete(Operation op) {
            latch.countDown();
          }
      });
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    try {
      if (!latch.await(operationTimeout, TimeUnit.MILLISECONDS)) {
        throw new OperationTimeoutException("Mutate operation timed out,"
            + "unable to modify counter [" + key + "]");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }
    getLogger().debug("Mutation returned %s", rv);
    return rv.get();
  }
  
  /**
   * Increment the given key by the given amount.
   *
   * Due to the way the memcached server operates on items, incremented and
   * decremented items will be returned as Strings with any operations that
   * return a value.
   *
   * @param key the key
   * @param by the amount to increment
   * @return the new value (-1 if the key doesn't exist)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long incr(String key, long by) {
    return mutate(null, Mutator.incr, key, by, 0, -1);
  }

  /**
   * Increment the given key by the given amount.
   *
   * Due to the way the memcached server operates on items, incremented and
   * decremented items will be returned as Strings with any operations that
   * return a value.
   *
   * @param key the key
   * @param by the amount to increment
   * @return the new value (-1 if the key doesn't exist)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long incr(String key, int by) {
    return mutate(null, Mutator.incr, key, (long)by, 0, -1);
  }

  /**
   * Decrement the given key by the given value.
   *
   * Due to the way the memcached server operates on items, incremented and
   * decremented items will be returned as Strings with any operations that
   * return a value.
   *
   * @param key the key
   * @param by the value
   * @return the new value (-1 if the key doesn't exist)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long decr(String key, long by) {
    return mutate(null, Mutator.decr, key, by, 0, -1);
  }

  /**
   * Decrement the given key by the given value.
   *
   * Due to the way the memcached server operates on items, incremented and
   * decremented items will be returned as Strings with any operations that
   * return a value.
   *
   * @param key the key
   * @param by the value
   * @return the new value (-1 if the key doesn't exist)
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long decr(String key, int by) {
    return mutate(null, Mutator.decr, key, (long)by, 0, -1);
  }

  /**
   * Increment the given counter, returning the new value.
   *
   * Due to the way the memcached server operates on items, incremented and
   * decremented items will be returned as Strings with any operations that
   * return a value.
   *
   * @param key the key
   * @param by the amount to increment
   * @param def the default value (if the counter does not exist)
   * @param exp the expiration of this object
   * @return the new value, or -1 if we were unable to increment or add
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long incr(String key, long by, long def, int exp) {
    return mutateWithDefault(null, Mutator.incr, key, by, def, exp);
  }

  /**
   * Increment the given counter, returning the new value.
   *
   * Due to the way the memcached server operates on items, incremented and
   * decremented items will be returned as Strings with any operations that
   * return a value.
   *
   * @param key the key
   * @param by the amount to increment
   * @param def the default value (if the counter does not exist)
   * @param exp the expiration of this object
   * @return the new value, or -1 if we were unable to increment or add
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long incr(String key, int by, long def, int exp) {
    return mutateWithDefault(null, Mutator.incr, key, (long)by, def, exp);
  }

  /**
   * Decrement the given counter, returning the new value.
   *
   * Due to the way the memcached server operates on items, incremented and
   * decremented items will be returned as Strings with any operations that
   * return a value.
   *
   * @param key the key
   * @param by the amount to decrement
   * @param def the default value (if the counter does not exist)
   * @param exp the expiration of this object
   * @return the new value, or -1 if we were unable to decrement or add
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long decr(String key, long by, long def, int exp) {
    return mutateWithDefault(null, Mutator.decr, key, by, def, exp);
  }

  /**
   * Decrement the given counter, returning the new value.
   *
   * Due to the way the memcached server operates on items, incremented and
   * decremented items will be returned as Strings with any operations that
   * return a value.
   *
   * @param key the key
   * @param by the amount to decrement
   * @param def the default value (if the counter does not exist)
   * @param exp the expiration of this object
   * @return the new value, or -1 if we were unable to decrement or add
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long decr(String key, int by, long def, int exp) {
    return mutateWithDefault(null, Mutator.decr, key, (long)by, def, exp);
  }

  protected long mutateWithDefault(MemcachedNode opNode, Mutator t, String key, long by, long def, int exp) {
    long rv = mutate(opNode, t, key, by, def, exp);
    // The ascii protocol doesn't support defaults, so I added them
    // manually here.
    if (rv == -1) {
      Future<CASResponse> f = asyncStore(StoreType.add, key, exp,
          String.valueOf(def));
      try {
        if (f.get(operationTimeout, TimeUnit.MILLISECONDS).type == CASResponseType.OK) {
          rv = def;
        } else {
          rv = mutate(opNode, t, key, by, 0, exp);
          assert rv != -1 : "Failed to mutate or init value";
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("Interrupted waiting for store", e);
      } catch (ExecutionException e) {
        if(e.getCause() instanceof CancellationException) {
          throw (CancellationException) e.getCause();
        } else {
          throw new RuntimeException("Failed waiting for store", e);
        }
      } catch (TimeoutException e) {
        throw new OperationTimeoutException("Timeout waiting to mutate or init"
          + " value" + buildTimeoutMessage(operationTimeout,
            TimeUnit.MILLISECONDS), e);
      }
    }
    return rv;
  }

  protected OperationFuture<CASLongResponse> asyncMutate(
      final MemcachedClientIF client, final MemcachedNode opNode,
      Mutator m, String key, long by, long def, int exp,
      final OperationListener<CASLongResponse> listener) {
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<CASLongResponse> rv = new OperationFuture<CASLongResponse>(key, latch, operationTimeout);
    final Operation op = opFact.mutate(m, key, by, def, exp,
        new OperationCallback() {
          @Override
          public void receivedStatus(Operation op, OperationStatus s) {
            rv.set(new CASLongResponse(s.isSuccess(), op.getResponseCas(), s.isSuccess() ? Long.parseLong(s.getMessage()) : -1), s);
          }

          @Override
          public void complete(Operation op) { onComplete(client, rv, op, latch, listener); }
        });
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    rv.setOperation(op);
    return rv;
  }

  /**
   * Asychronous increment.
   *
   * @param key key to increment
   * @param by the amount to increment the value by
   * @return a future with the incremented value, or -1 if the increment failed.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASLongResponse> asyncIncr(String key, long by, OperationListener<CASLongResponse> listener) {
    return asyncMutate(this, null, Mutator.incr, key, by, 0, -1, listener);
  }

  /**
   * Asychronous increment.
   *
   * @param key key to increment
   * @param by the amount to increment the value by
   * @return a future with the incremented value, or -1 if the increment failed.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASLongResponse> asyncIncr(String key, int by, OperationListener<CASLongResponse> listener) {
    return asyncMutate(this, null, Mutator.incr, key, (long)by, 0, -1, listener);
  }

  /**
   * Asynchronous decrement.
   *
   * @param key key to increment
   * @param by the amount to increment the value by
   * @return a future with the decremented value, or -1 if the increment failed.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASLongResponse> asyncDecr(String key, long by, OperationListener<CASLongResponse> listener) {
    return asyncMutate(this, null, Mutator.decr, key, by, 0, -1, listener);
  }

  /**
   * Asynchronous decrement.
   *
   * @param key key to increment
   * @param by the amount to increment the value by
   * @return a future with the decremented value, or -1 if the increment failed.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASLongResponse> asyncDecr(String key, int by, OperationListener<CASLongResponse> listener) {
    return asyncMutate(this, null, Mutator.decr, key, (long)by, 0, -1, listener);
  }

  /**
   * Asychronous increment.
   *
   * @param key key to increment
   * @param by the amount to increment the value by
   * @return a future with the incremented value, or -1 if the increment failed.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASLongResponse> asyncIncr(String key, long by) {
    return asyncMutate(this, null, Mutator.incr, key, by, 0, -1, null);
  }

  /**
   * Asychronous increment.
   *
   * @param key key to increment
   * @param by the amount to increment the value by
   * @return a future with the incremented value, or -1 if the increment failed.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASLongResponse> asyncIncr(String key, int by) {
    return asyncMutate(this, null, Mutator.incr, key, (long)by, 0, -1, null);
  }

  /**
   * Asynchronous decrement.
   *
   * @param key key to increment
   * @param by the amount to increment the value by
   * @return a future with the decremented value, or -1 if the increment failed.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASLongResponse> asyncDecr(String key, long by) {
    return asyncMutate(this, null, Mutator.decr, key, by, 0, -1, null);
  }

  /**
   * Asynchronous decrement.
   *
   * @param key key to increment
   * @param by the amount to increment the value by
   * @return a future with the decremented value, or -1 if the increment failed.
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASLongResponse> asyncDecr(String key, int by) {
    return asyncMutate(this, null, Mutator.decr, key, (long)by, 0, -1, null);
  }

  /**
   * Increment the given counter, returning the new value.
   *
   * @param key the key
   * @param by the amount to increment
   * @param def the default value (if the counter does not exist)
   * @return the new value, or -1 if we were unable to increment or add
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long incr(String key, long by, long def) {
    return mutateWithDefault(null, Mutator.incr, key, by, def, 0);
  }

  /**
   * Increment the given counter, returning the new value.
   *
   * @param key the key
   * @param by the amount to increment
   * @param def the default value (if the counter does not exist)
   * @return the new value, or -1 if we were unable to increment or add
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long incr(String key, int by, long def) {
    return mutateWithDefault(null, Mutator.incr, key, (long)by, def, 0);
  }

  /**
   * Decrement the given counter, returning the new value.
   *
   * @param key the key
   * @param by the amount to decrement
   * @param def the default value (if the counter does not exist)
   * @return the new value, or -1 if we were unable to decrement or add
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long decr(String key, long by, long def) {
    return mutateWithDefault(null, Mutator.decr, key, by, def, 0);
  }

  /**
   * Decrement the given counter, returning the new value.
   *
   * @param key the key
   * @param by the amount to decrement
   * @param def the default value (if the counter does not exist)
   * @return the new value, or -1 if we were unable to decrement or add
   * @throws OperationTimeoutException if the global operation timeout is
   *           exceeded
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public long decr(String key, int by, long def) {
    return mutateWithDefault(null, Mutator.decr, key, (long)by, def, 0);
  }

  /**
   * Delete the given key from the cache.
   *
   * @param key the key to delete
   * @return whether or not the operation was performed
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> delete(String key) {
    return delete(key, 0L);
  }

  /**
   * Delete the given key from the cache of the given CAS value applies.
   *
   *
   * @param key the key to delete
   * @param cas the CAS value to apply.
   * @return whether or not the operation was performed
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> delete(String key, long cas) {
    return delete(key, null);
  }

  /**
   * Delete the given key from the cache.
   *
   * @param key the key to delete
   * @return whether or not the operation was performed
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<CASResponse> delete(String key, final OperationListener<CASResponse> listener) {
    return delete(this, null, false, key, 0, listener);
  }

  protected OperationFuture<CASResponse> delete(
      final MemcachedClientIF client, final MemcachedNode opNode,
      boolean quite, String key, long cas, final OperationListener<CASResponse> listener) {
    final CountDownLatch latch = new CountDownLatch(1);
    final OperationFuture<CASResponse> rv = new OperationFuture<CASResponse>(key,
            latch, operationTimeout);
    final DeleteOperation op = quite
        ? opFact.deleteQuiet(key, cas)
        : opFact.delete(key, cas, new DataCallback() {
      @Override
      public void gotData(String k, int flags, long cas, byte[] data) {
        rv.setCas(cas);
      }

      @Override
      public void receivedStatus(Operation op, OperationStatus s) {
        rv.set(new CASResponse(s.isSuccess(), op.getResponseCas()), s);
      }

      @Override
      public void complete(Operation op) { onComplete(client, rv, op, latch, listener); }
    });
    rv.setOperation(op);
    if (opNode == null) mconn.enqueueOperation(key, op);
    else mconn.enqueueOperation(opNode, op);
    return rv;
  }

  /**
   * Flush all caches from all servers with a delay of application.
   *
   * @param delay the period of time to delay, in seconds
   * @return whether or not the operation was accepted
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<Boolean> flush(final int delay) {
    final AtomicReference<Boolean> flushResult =
        new AtomicReference<Boolean>(null);
    final ConcurrentLinkedQueue<Operation> ops =
        new ConcurrentLinkedQueue<Operation>();
    final CountDownLatch blatch = broadcastOp(new BroadcastOpFactory() {
      public Operation newOp(final MemcachedNode n,
          final CountDownLatch latch) {
        final Operation op = opFact.flush(delay, new OperationCallback() {
          @Override
          public void receivedStatus(Operation op, OperationStatus s) {
            flushResult.set(s.isSuccess());
          }

          @Override
          public void complete(Operation op) {
            latch.countDown();
          }
        });
        ops.add(op);
        return op;
      }
    });

    return new OperationFuture<Boolean>(null, blatch, flushResult,
        operationTimeout) {
      @Override
      public boolean cancel(boolean ign) {
        boolean rv = false;
        for (Operation op : ops) {
          op.cancel();
          rv |= op.getState() == OperationState.WRITE_QUEUED;
        }
        return rv;
      }

      @Override
      public Boolean get(long duration, TimeUnit units)
        throws InterruptedException, TimeoutException, ExecutionException {
        status = new OperationStatus(true, "OK");
        return super.get(duration, units);
      }

      @Override
      public boolean isCancelled() {
        boolean rv = false;
        for (Operation op : ops) {
          rv |= op.isCancelled();
        }
        return rv;
      }

      @Override
      public boolean isDone() {
        boolean rv = true;
        for (Operation op : ops) {
          rv &= op.getState() == OperationState.COMPLETE;
        }
        return rv || isCancelled();
      }
    };
  }

  /**
   * Flush all caches from all servers immediately.
   *
   * @return whether or not the operation was performed
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public OperationFuture<Boolean> flush() {
    return flush(-1);
  }

  @Override
  public Set<String> listSaslMechanisms() {
    final ConcurrentMap<String, String> rv =
        new ConcurrentHashMap<String, String>();

    final CountDownLatch blatch = broadcastOp(new BroadcastOpFactory() {
      public Operation newOp(MemcachedNode n, final CountDownLatch latch) {
        return opFact.saslMechs(new OperationCallback() {
          @Override
          public void receivedStatus(Operation op, OperationStatus status) {
            for (String s : status.getMessage().split(" ")) {
              rv.put(s, s);
            }
          }

          @Override
          public void complete(Operation op) {
            latch.countDown();
          }
        });
      }
    });

    try {
      blatch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    return rv.keySet();
  }

  /**
   * Shut down immediately.
   */
  @Override
  public void shutdown() {
    shutdown(-1, TimeUnit.MILLISECONDS);
  }

  /**
   * Shut down this client gracefully.
   *
   * @param timeout the amount of time time for shutdown
   * @param unit the TimeUnit for the timeout
   * @return result of the shutdown request
   */
  @Override
  public boolean shutdown(long timeout, TimeUnit unit) {
    // Guard against double shutdowns (bug 8).
    if (shuttingDown) {
      getLogger().info("Suppressing duplicate attempt to shut down");
      return false;
    }
    shuttingDown = true;
    String baseName = mconn.getName();
    mconn.setName(baseName + " - SHUTTING DOWN");
    boolean rv = true;
    try {
      // Conditionally wait
      if (timeout > 0) {
        mconn.setName(baseName + " - SHUTTING DOWN (waiting)");
        rv = waitForQueues(timeout, unit);
      }
    } finally {
      // But always begin the shutdown sequence
      try {
        mconn.setName(baseName + " - SHUTTING DOWN (telling client)");
        mconn.shutdown();
        mconn.setName(baseName + " - SHUTTING DOWN (informed client)");
        tcService.shutdown();
      } catch (IOException e) {
        getLogger().warn("exception while shutting down", e);
      }
    }
    return rv;
  }

  /**
   * Wait for the queues to die down.
   *
   * @param timeout the amount of time time for shutdown
   * @param unit the TimeUnit for the timeout
   * @return result of the request for the wait
   * @throws IllegalStateException in the rare circumstance where queue is too
   *           full to accept any more requests
   */
  @Override
  public boolean waitForQueues(long timeout, TimeUnit unit) {
    final CountDownLatch blatch = broadcastOp(new BroadcastOpFactory() {
      public Operation newOp(final MemcachedNode n,
          final CountDownLatch latch) {
        return opFact.noop(new OperationCallback() {
          @Override
          public void complete(Operation op) {
            latch.countDown();
          }

          @Override
          public void receivedStatus(Operation op, OperationStatus s) {
            // Nothing special when receiving status, only
            // necessary to complete the interface
          }
        });
      }
    }, mconn.getLocator().getAll(), false);
    try {
      // XXX: Perhaps IllegalStateException should be caught here
      // and the check retried.
      return blatch.await(timeout, unit);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for queues", e);
    }
  }

  /**
   * Add a connection observer.
   *
   * If connections are already established, your observer will be called with
   * the address and -1.
   *
   * @param obs the ConnectionObserver you wish to add
   * @return true if the observer was added.
   */
  @Override
  public boolean addObserver(ConnectionObserver obs) {
    boolean rv = mconn.addObserver(obs);
    if (rv) {
      for (MemcachedNode node : mconn.getLocator().getAll()) {
        if (node.isActive()) {
          obs.connectionEstablished(node.getSocketAddress(), -1);
        }
      }
    }
    return rv;
  }

  /**
   * Remove a connection observer.
   *
   * @param obs the ConnectionObserver you wish to add
   * @return true if the observer existed, but no longer does
   */
  @Override
  public boolean removeObserver(ConnectionObserver obs) {
    return mconn.removeObserver(obs);
  }

  @Override
  public void connectionEstablished(SocketAddress sa, int reconnectCount) {
    if (authDescriptor != null) {
      if (authDescriptor.authThresholdReached()) {
        this.shutdown();
      }
      authMonitor.authConnection(mconn, opFact, authDescriptor, findNode(sa));
    }
  }

  private MemcachedNode findNode(SocketAddress sa) {
    MemcachedNode node = null;
    for (MemcachedNode n : mconn.getLocator().getAll()) {
      if (n.getSocketAddress().equals(sa)) {
        node = n;
      }
    }
    assert node != null : "Couldn't find node connected to " + sa;
    return node;
  }

  private String buildTimeoutMessage(long timeWaited, TimeUnit unit) {
    final StringBuilder message = new StringBuilder();
    message.append(MessageFormat.format("waited {0} ms.",
      unit.convert(timeWaited, TimeUnit.MILLISECONDS)));
    message.append(" Node status: ").append(mconn.connectionsStatus());
    return message.toString();
  }

  @Override
  public void connectionLost(SocketAddress sa) {
    // Don't care.
  }

  @Override
  public String toString() {
    return connFactory.toString();
  }
}
