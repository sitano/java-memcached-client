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

import net.spy.memcached.internal.*;
import net.spy.memcached.ops.*;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.Transcoder;
import net.spy.memcached.util.StringUtils;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;

class MemcachedGroupKeyClient implements MemcachedClientIF {
  protected final MemcachedClient client;

  protected final String groupKey;
  protected final MemcachedNode groupNode;

  MemcachedGroupKeyClient(MemcachedClient client, String groupKey) {
    this.client = client;

    this.groupKey = groupKey;
    this.groupNode = client.getMemcachedConnection().selectNode(groupKey);

    if (this.groupKey == null || this.groupNode == null)
      throw new IllegalArgumentException("selectNode(" + groupKey + ") failed to select a node for GroupKeyClient");
  }

  MemcachedGroupKeyClient(MemcachedClient client, String groupKey, MemcachedNode groupNode) {
    this.client = client;

    this.groupKey = groupKey;
    this.groupNode = groupNode;

      if (this.groupKey == null || this.groupNode == null)
      throw new IllegalArgumentException("groupNode can't be null inside GroupKeyClient constructor");
  }

  public Collection<SocketAddress> getAvailableServers() {
    return client.getAvailableServers();
  }

  public Collection<SocketAddress> getUnavailableServers() {
    return client.getUnavailableServers();
  }

  public NodeLocator getNodeLocator() {
    return client.getNodeLocator();
  }

  public MemcachedNode getGroupNode() {
    return groupNode;
  }

  public MemcachedClient getClient() {
    return client;
  }

  public String getGroupKey() {
    return groupKey;
  }

  public MemcachedClientIF getGroupKey(String key) {
    StringUtils.validateKey(key, client.getOperationFactory() instanceof BinaryOperationFactory);
    if (key.equals(groupKey)) return this;
    return new MemcachedGroupKeyClient(client, key);
  }

  public MemcachedClientIF getQuietClient() {
    return new MemcachedGroupKeyQuietClient(client, groupKey, groupNode);
  }

  public Transcoder<Object> getTranscoder() {
    return client.getTranscoder();
  }

  private OperationFuture<CASResponse> asyncStore(StoreType storeType, String key, int exp, Object value) {
    return client.asyncStore(this, groupNode, storeType, key, exp, value, client.getTranscoder(), null);
  }

  public <T> OperationFuture<CASResponse> touch(final String key, final int exp) {
    return touch(key, exp, client.getTranscoder(), null);
  }

  public <T> Future<CASResponse> touch(String key, int exp, Transcoder<T> tc) {
    return touch(key, exp, client.getTranscoder(), null);
  }

  public <T> OperationFuture<CASResponse> touch(final String key, final int exp, final Transcoder<T> tc, final OperationListener<CASResponse> listener) {
    return client.touch(this, groupNode, key, exp, tc, listener);
  }

  public OperationFuture<CASResponse> append(long cas, String key, Object val) {
    return append(cas, key, val, client.getTranscoder(), null);
  }

  public <T> OperationFuture<CASResponse> append(long cas, String key, T val, Transcoder<T> tc) {
    return client.asyncCat(this, groupNode, ConcatenationType.append, cas, key, val, tc, null);
  }

  public <T> OperationFuture<CASResponse> append(long cas, String key, T val, Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return client.asyncCat(this, groupNode, ConcatenationType.append, cas, key, val, tc, listener);
  }

  public OperationFuture<CASResponse> prepend(long cas, String key, Object val) {
    return prepend(cas, key, val, client.getTranscoder(), null);
  }

  public <T> OperationFuture<CASResponse> prepend(long cas, String key, T val, Transcoder<T> tc) {
    return client.asyncCat(this, groupNode, ConcatenationType.prepend, cas, key, val, tc, null);
  }

  public <T> OperationFuture<CASResponse> prepend(long cas, String key, T val, Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return client.asyncCat(this, groupNode, ConcatenationType.prepend, cas, key, val, tc, listener);
  }

  public <T> Future<CASResponse> asyncCAS(String key, long casId, T value, Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return asyncCAS(key, casId, 0, value, tc, listener);
  }

  public <T> Future<CASResponse> asyncCAS(String key, long casId, T value, Transcoder<T> tc) {
    return asyncCAS(key, casId, 0, value, tc, null);
  }

  public <T> Future<CASResponse> asyncCAS(String key, long casId, int exp, T value, Transcoder<T> tc, final OperationListener<CASResponse> listener) {
    return client.asyncCAS(this, groupNode, key, casId, exp, value, tc, listener);
  }

  public <T> Future<CASResponse> asyncCAS(String key, long casId, int exp,
                                          T value, Transcoder<T> tc) {
    return asyncCAS(key, casId, exp, value, tc, null);
  }

  public Future<CASResponse> asyncCAS(String key, long casId, Object value) {
    return asyncCAS(key, casId, value, client.getTranscoder());
  }

  public <T> CASResponse cas(String key, long casId, T value,
                             Transcoder<T> tc) {
    return cas(key, casId, 0, value, tc);
  }

  public <T> CASResponse cas(String key, long casId, int exp, T value,
      Transcoder<T> tc) {
    try {
      return asyncCAS(key, casId, exp, value, tc).get(client.getOperationTimeout(),
              TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Exception waiting for value", e);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value", e);
    }
  }

  public CASResponse cas(String key, long casId, Object value) {
    return cas(key, casId, value, client.getTranscoder());
  }

  public <T> OperationFuture<CASResponse> add(String key, int exp, T o,
      Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return client.asyncStore(this, groupNode, StoreType.add, key, exp, o, tc, listener);
  }

  public <T> OperationFuture<CASResponse> add(String key, int exp, T o,
      Transcoder<T> tc) {
    return client.asyncStore(this, groupNode, StoreType.add, key, exp, o, tc, null);
  }

  public OperationFuture<CASResponse> add(String key, int exp, Object o) {
    return client.asyncStore(this, groupNode, StoreType.add, key, exp, o, client.getTranscoder(), null);
  }

  public <T> OperationFuture<CASResponse> set(String key, int exp, T o,
      Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return client.asyncStore(this, groupNode, StoreType.set, key, exp, o, tc, listener);
  }

  public <T> OperationFuture<CASResponse> set(String key, int exp, T o,
      Transcoder<T> tc) {
    return client.asyncStore(this, groupNode, StoreType.set, key, exp, o, tc, null);
  }

  public OperationFuture<CASResponse> set(String key, int exp, Object o) {
    return client.asyncStore(this, groupNode, StoreType.set, key, exp, o, client.getTranscoder(), null);
  }

  public <T> OperationFuture<CASResponse> replace(String key, int exp, T o,
      Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return client.asyncStore(this, groupNode, StoreType.replace, key, exp, o, tc, listener);
  }

  public <T> OperationFuture<CASResponse> replace(String key, int exp, T o,
      Transcoder<T> tc) {
    return client.asyncStore(this, groupNode, StoreType.replace, key, exp, o, tc, null);
  }

  public OperationFuture<CASResponse> replace(String key, int exp, Object o) {
    return client.asyncStore(this, groupNode, StoreType.replace, key, exp, o, client.getTranscoder(), null);
  }

  public <T> GetFuture<T> asyncGet(final String key, final Transcoder<T> tc, final OperationListener<T> listener) {
    return client.asyncGet(this, groupNode, key, tc, listener);
  }

  public <T> GetFuture<T> asyncGet(final String key, final Transcoder<T> tc) {
    return asyncGet(key, tc, null);
  }

  public GetFuture<Object> asyncGet(final String key) {
    return asyncGet(key, client.getTranscoder());
  }

  public <T> OperationFuture<CASValue<T>> asyncGets(final String key,
      final Transcoder<T> tc, final OperationListener<CASValue<T>> listener) {
    return client.asyncGets(this, groupNode, key, tc, listener);
  }

  public <T> OperationFuture<CASValue<T>> asyncGets(final String key,
      final Transcoder<T> tc) {
    return asyncGets(key, tc, null);
  }

  public OperationFuture<CASValue<Object>> asyncGets(final String key) {
    return asyncGets(key, client.getTranscoder());
  }

  public <T> CASValue<T> gets(String key, Transcoder<T> tc) {
    try {
      return asyncGets(key, tc).get(client.getOperationTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Exception waiting for value", e);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value", e);
    }
  }

  public <T> CASValue<T> getAndTouch(String key, int exp, Transcoder<T> tc) {
    try {
      return asyncGetAndTouch(key, exp, tc).get(client.getOperationTimeout(),
              TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Exception waiting for value", e);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value", e);
    }
  }

  public CASValue<Object> getAndTouch(String key, int exp) {
    return getAndTouch(key, exp, client.getTranscoder());
  }

  public CASValue<Object> gets(String key) {
    return gets(key, client.getTranscoder());
  }

  public <T> T get(String key, Transcoder<T> tc) {
    try {
      return asyncGet(key, tc).get(client.getOperationTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted waiting for value", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Exception waiting for value", e);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for value", e);
    }
  }

  public Object get(String key) {
    return get(key, client.getTranscoder());
  }

  @SuppressWarnings("unchecked")
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
      Iterator<Transcoder<T>> tcIter, final OperationListener<Map<String, T>> listener) {
    final MemcachedGroupKeyClient client = this;
    final Map<String, Future<T>> m = new ConcurrentHashMap<String, Future<T>>();

    // This map does not need to be a ConcurrentHashMap
    // because it is fully populated when it is used and
    // used only to read the transcoder for a key.
    final Map<String, Transcoder<T>> tcMap = new HashMap<String, Transcoder<T>>();

    final Collection<String> keys = new ArrayList<String>();
    while (keyIter.hasNext() && tcIter.hasNext()) {
        String key = keyIter.next();
        keys.add(key);
        tcMap.put(key, tcIter.next());
    }

    final CountDownLatch latch = new CountDownLatch(1);
    final Collection<Operation> ops = new ArrayList<Operation>(1);
    final BulkGetFuture<T> rv = new BulkGetFuture<T>(m, ops, latch);

    GetOperation.Callback cb = new GetOperation.Callback() {
      @SuppressWarnings("synthetic-access")
      public void receivedStatus(Operation op, OperationStatus status) {
        rv.setStatus(status);
      }

      public void gotData(String k, int flags, byte[] data) {
        Transcoder<T> tc = tcMap.get(k);
        m.put(k, client.client.getTranscodeService().decode(tc, new CachedData(flags, data, tc.getMaxSize())));
      }

      public void complete(Operation op) {
        latch.countDown();
        if (listener != null && latch.getCount() < 1) listener.onComplete(client, rv.getStatus(), rv);
      }
    };

    // Now that we know how many servers it breaks down into, and the latch
    // is all set up, convert all of these strings collections to operations
    final Map<MemcachedNode, Operation> mops = new HashMap<MemcachedNode, Operation>();

    Operation op = client.client.getOperationFactory().get(keys, cb);
    mops.put(groupNode, op);
    ops.add(op);

    client.client.getMemcachedConnection().checkState();
    client.client.getMemcachedConnection().addOperations(mops);

    return rv;
  }

  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter,
      Iterator<Transcoder<T>> tcIter, final OperationListener<Map<String, CASValue<T>>> listener) {
    final MemcachedGroupKeyClient client = this;
    final Map<String, Future<CASValue<T>>> m = new ConcurrentHashMap<String, Future<CASValue<T>>>();

    // This map does not need to be a ConcurrentHashMap
    // because it is fully populated when it is used and
    // used only to read the transcoder for a key.
    final Map<String, Transcoder<T>> tcMap = new HashMap<String, Transcoder<T>>();

    // Break the gets down into groups by key
    final Collection<String> keys = new ArrayList<String>();
      while (keyIter.hasNext() && tcIter.hasNext()) {
          String key = keyIter.next();
          keys.add(key);
          tcMap.put(key, tcIter.next());
      }

    final CountDownLatch latch = new CountDownLatch(1);
    final Collection<Operation> ops = new ArrayList<Operation>(1);
    final BulkGetFuture<CASValue<T>> rv = new BulkGetFuture<CASValue<T>>(m, ops, latch);

    GetsOperation.Callback cb = new GetsOperation.Callback() {
      @SuppressWarnings("synthetic-access")
      public void receivedStatus(Operation op, OperationStatus status) {
        rv.setStatus(status);
      }

      public void gotData(String k, int flags, long cas, byte[] data) {
        assert cas > 0 : "CAS was less than zero:  " + cas;
        Transcoder<T> tc = tcMap.get(k);
        m.put(k, client.client.getTranscodeService().decodes(tc, new CachedData(flags, data, tc.getMaxSize()), cas));
      }

      public void complete(Operation op) {
        latch.countDown();
        if (listener != null && latch.getCount() < 1) listener.onComplete(client, rv.getStatus(), rv);
      }
    };

    // Now that we know how many servers it breaks down into, and the latch
    // is all set up, convert all of these strings collections to operations
    final Map<MemcachedNode, Operation> mops = new HashMap<MemcachedNode, Operation>();

    Operation op = client.client.getOperationFactory().gets(keys, cb);
    mops.put(groupNode, op);
    ops.add(op);

    client.client.getMemcachedConnection().checkState();
    client.client.getMemcachedConnection().addOperations(mops);

    return rv;
  }

  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter, Iterator<Transcoder<T>> tcIter) {
    return asyncGetsBulk(keyIter, tcIter, null);
  }

  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys, Iterator<Transcoder<T>> tcIter,
                                                                OperationListener<Map<String, CASValue<T>>> listener) {
    return asyncGetsBulk(keys.iterator(), tcIter, listener);
  }

  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys, Iterator<Transcoder<T>> tcIter) {
    return asyncGetsBulk(keys.iterator(), tcIter, null);
  }

  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter, Transcoder<T> tc,
                                                                OperationListener<Map<String, CASValue<T>>> listener) {
    return asyncGetsBulk(keyIter, new SingleElementInfiniteIterator<Transcoder<T>>(tc), listener);
  }

  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter, Transcoder<T> tc) {
    return asyncGetsBulk(keyIter,
            new SingleElementInfiniteIterator<Transcoder<T>>(tc));
  }

  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys, Transcoder<T> tc,
                                                                OperationListener<Map<String, CASValue<T>>> listener) {
    return asyncGetsBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), listener);
  }

  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys, Transcoder<T> tc) {
    return asyncGetsBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), null);
  }

  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(
          Iterator<String> keyIter, OperationListener<Map<String, CASValue<Object>>> listener) {
    return asyncGetsBulk(keyIter, client.getTranscoder(), listener);
  }

  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(
          Iterator<String> keyIter) {
    return asyncGetsBulk(keyIter, client.getTranscoder());
  }

  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(Collection<String> keys, OperationListener<Map<String, CASValue<Object>>> listener) {
    return asyncGetsBulk(keys, client.getTranscoder(), listener);
  }

  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(Collection<String> keys) {
    return asyncGetsBulk(keys, client.getTranscoder());
  }

  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
                                                     Iterator<Transcoder<T>> tcIter) {
    return asyncGetBulk(keyIter, tcIter, null);
  }

  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Iterator<Transcoder<T>> tcIter,
                                                     OperationListener<Map<String, T>> listener) {
    return asyncGetBulk(keys.iterator(), tcIter, listener);
  }

  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
                                                     Iterator<Transcoder<T>> tcIter) {
    return asyncGetBulk(keys.iterator(), tcIter, null);
  }

  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter, Transcoder<T> tc,
                                                     OperationListener<Map<String, T>> listener) {
    return asyncGetBulk(keyIter, new SingleElementInfiniteIterator<Transcoder<T>>(tc), listener);
  }

  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter, Transcoder<T> tc) {
    return asyncGetBulk(keyIter,
            new SingleElementInfiniteIterator<Transcoder<T>>(tc));
  }

  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Transcoder<T> tc,
                                                     OperationListener<Map<String, T>> listener) {
    return asyncGetBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), listener);
  }

  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys, Transcoder<T> tc) {
    return asyncGetBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), null);
  }

  public BulkFuture<Map<String, Object>> asyncGetBulk(
          Iterator<String> keyIter, OperationListener<Map<String, Object>> listener) {
    return asyncGetBulk(keyIter, client.getTranscoder(), listener);
  }

  public BulkFuture<Map<String, Object>> asyncGetBulk(
          Iterator<String> keyIter) {
    return asyncGetBulk(keyIter, client.getTranscoder());
  }

  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys, OperationListener<Map<String, Object>> listener) {
    return asyncGetBulk(keys, client.getTranscoder(), listener);
  }

  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
    return asyncGetBulk(keys, client.getTranscoder());
  }

  public <T> BulkFuture<Map<String, T>> asyncGetBulk(OperationListener<Map<String, T>> listener, Transcoder<T> tc, String... keys) {
    return asyncGetBulk(Arrays.asList(keys), tc, listener);
  }

  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> tc,
                                                     String... keys) {
    return asyncGetBulk(Arrays.asList(keys), tc);
  }

  public BulkFuture<Map<String, Object>> asyncGetBulk(OperationListener<Map<String, Object>> listener, String... keys) {
    return asyncGetBulk(Arrays.asList(keys), client.getTranscoder(), listener);
  }

  public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys) {
    return asyncGetBulk(Arrays.asList(keys), client.getTranscoder());
  }

  public OperationFuture<CASValue<Object>> asyncGetAndTouch(String key, int exp) {
    return asyncGetAndTouch(key, exp, client.getTranscoder());
  }

  public <T> OperationFuture<CASValue<T>> asyncGetAndTouch( String key, int exp,  Transcoder<T> tc) {
    return asyncGetAndTouch(key, exp, tc, null);
  }

  public <T> OperationFuture<CASValue<T>> asyncGetAndTouch( String key, int exp,  Transcoder<T> tc,
                                                            OperationListener<CASValue<T>> listener) {
    return client.asyncGetAndTouch(this, groupNode, key, exp, tc, listener);
  }

  public <T> Map<String, T> getBulk(Iterator<String> keyIter,
                                    Transcoder<T> tc) {
    try {
      return asyncGetBulk(keyIter, tc).get(client.getOperationTimeout(),
              TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted getting bulk values", e);
    } catch (ExecutionException e) {
      throw new RuntimeException("Failed getting bulk values", e);
    } catch (TimeoutException e) {
      throw new OperationTimeoutException("Timeout waiting for bulkvalues", e);
    }
  }

  public Map<String, Object> getBulk(Iterator<String> keyIter) {
    return getBulk(keyIter, client.getTranscoder());
  }

  public <T> Map<String, T> getBulk(Collection<String> keys,
                                    Transcoder<T> tc) {
    return getBulk(keys.iterator(), tc);
  }

  public Map<String, Object> getBulk(Collection<String> keys) {
    return getBulk(keys, client.getTranscoder());
  }

  public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) {
    return getBulk(Arrays.asList(keys), tc);
  }

  public Map<String, Object> getBulk(String... keys) {
    return getBulk(Arrays.asList(keys), client.getTranscoder());
  }

  public Map<SocketAddress, String> getVersions() {
    return client.getVersions();
  }

  public Map<SocketAddress, Map<String, String>> getStats() {
    return getStats(null);
  }

  public Map<SocketAddress, Map<String, String>> getStats(String arg) {
    return client.getStats(arg);
  }

  public long incr(String key, long by) {
    return client.mutate(groupNode, Mutator.incr, key, by, 0, -1);
  }

  public long incr(String key, int by) {
    return client.mutate(groupNode, Mutator.incr, key, (long) by, 0, -1);
  }

  public long decr(String key, long by) {
    return client.mutate(groupNode, Mutator.decr, key, by, 0, -1);
  }

  public long decr(String key, int by) {
    return client.mutate(groupNode, Mutator.decr, key, (long) by, 0, -1);
  }

  public long incr(String key, long by, long def, int exp) {
    return client.mutateWithDefault(groupNode, Mutator.incr, key, by, def, exp);
  }

  public long incr(String key, int by, long def, int exp) {
    return client.mutateWithDefault(groupNode, Mutator.incr, key, (long) by, def, exp);
  }

  public long decr(String key, long by, long def, int exp) {
    return client.mutateWithDefault(groupNode, Mutator.decr, key, by, def, exp);
  }

  public long decr(String key, int by, long def, int exp) {
    return client.mutateWithDefault(groupNode, Mutator.decr, key, (long) by, def, exp);
  }

  public OperationFuture<CASLongResponse> asyncIncr(String key, long by, OperationListener<CASLongResponse> listener) {
    return client.asyncMutate(this, groupNode, Mutator.incr, key, by, 0, -1, listener);
  }

  public OperationFuture<CASLongResponse> asyncIncr(String key, int by, OperationListener<CASLongResponse> listener) {
    return client.asyncMutate(this, groupNode, Mutator.incr, key, (long) by, 0, -1, listener);
  }

  public OperationFuture<CASLongResponse> asyncDecr(String key, long by, OperationListener<CASLongResponse> listener) {
    return client.asyncMutate(this, groupNode, Mutator.decr, key, by, 0, -1, listener);
  }

  public OperationFuture<CASLongResponse> asyncDecr(String key, int by, OperationListener<CASLongResponse> listener) {
    return client.asyncMutate(this, groupNode, Mutator.decr, key, (long) by, 0, -1, listener);
  }

  public OperationFuture<CASLongResponse> asyncIncr(String key, long by) {
    return client.asyncMutate(this, groupNode, Mutator.incr, key, by, 0, -1, null);
  }

  public OperationFuture<CASLongResponse> asyncIncr(String key, int by) {
    return client.asyncMutate(this, groupNode, Mutator.incr, key, (long) by, 0, -1, null);
  }

  public OperationFuture<CASLongResponse> asyncDecr(String key, long by) {
    return client.asyncMutate(this, groupNode, Mutator.decr, key, by, 0, -1, null);
  }

  public OperationFuture<CASLongResponse> asyncDecr(String key, int by) {
    return client.asyncMutate(this, groupNode, Mutator.decr, key, (long) by, 0, -1, null);
  }

  public long incr(String key, long by, long def) {
    return client.mutateWithDefault(groupNode, Mutator.incr, key, by, def, 0);
  }

  public long incr(String key, int by, long def) {
    return client.mutateWithDefault(groupNode, Mutator.incr, key, (long) by, def, 0);
  }

  public long decr(String key, long by, long def) {
    return client.mutateWithDefault(groupNode, Mutator.decr, key, by, def, 0);
  }

  public long decr(String key, int by, long def) {
    return client.mutateWithDefault(groupNode, Mutator.decr, key, (long) by, def, 0);
  }

  public OperationFuture<CASResponse> delete(String key) {
    return delete(key, null);
  }

  public OperationFuture<CASResponse> delete(String key, final OperationListener<CASResponse> listener) {
    return delete(false, key, listener);
  }

  protected OperationFuture<CASResponse> delete(boolean quite,
      String key, final OperationListener<CASResponse> listener) {
    return client.delete(this, groupNode, quite, key, listener);
  }

  public OperationFuture<Boolean> flush(final int delay) {
    return client.flush(delay);
  }

  public OperationFuture<Boolean> flush() {
    return client.flush();
  }

  public Set<String> listSaslMechanisms() {
    return client.listSaslMechanisms();
  }

  public void shutdown() {
    throw new UnsupportedOperationException();
  }

  public boolean shutdown(long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  public boolean waitForQueues(long timeout, TimeUnit unit) {
    return client.waitForQueues(timeout, unit);
  }

  public boolean addObserver(ConnectionObserver obs) {
    return client.addObserver(obs);
  }

  public boolean removeObserver(ConnectionObserver obs) {
    return client.removeObserver(obs);
  }
}
