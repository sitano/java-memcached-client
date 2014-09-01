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

import net.spy.memcached.internal.BulkFuture;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.SingleElementInfiniteIterator;
import net.spy.memcached.ops.StoreType;
import net.spy.memcached.transcoders.Transcoder;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

class MemcachedGroupKeyQuietClient extends MemcachedGroupKeyClient {
  MemcachedGroupKeyQuietClient(MemcachedClient client, String groupKey, MemcachedNode groupNode) {
    super(client, groupKey, groupNode);
  }

  MemcachedGroupKeyQuietClient(MemcachedClient client, String groupKey) {
    super(client, groupKey);
  }

  @Override
  public Collection<SocketAddress> getAvailableServers() {
    return client.getAvailableServers();
  }

  @Override
  public Collection<SocketAddress> getUnavailableServers() {
    return client.getUnavailableServers();
  }

  @Override
  public NodeLocator getNodeLocator() {
    return client.getNodeLocator();
  }

  @Override
  public MemcachedClientIF getGroupKey(String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public MemcachedClientIF getQuietClient() {
    return this;
  }

  @Override
  public Transcoder<Object> getTranscoder() {
    return client.getTranscoder();
  }

  private OperationFuture<CASResponse> asyncStore(StoreType storeType, String key, int exp, Object value) {
    throw new UnsupportedOperationException(); // TODO: return client.asyncStore(groupNode, storeType, key, exp, value, client.getTranscoder(), null);
  }

  @Override
  public <T> OperationFuture<CASResponse> touch(final String key, final int exp) {
    return touch(key, exp, client.getTranscoder(), null);
  }

  @Override
  public <T> Future<CASResponse> touch(String key, int exp, Transcoder<T> tc) {
    return touch(key, exp, client.getTranscoder(), null);
  }

  @Override
  public <T> OperationFuture<CASResponse> touch(final String key, final int exp,
                                                final Transcoder<T> tc, final OperationListener<CASResponse> listener) {
    throw new UnsupportedOperationException(); // TODO: clent.touch(groupNode, key, exp, tc, listener);
  }

  @Override
  public Future<CASResponse> append(String key, Object val) {
    return append(0, key, val, client.getTranscoder(), null);
  }

  @Override
  public <T> Future<CASResponse> append(String key, T val, Transcoder<T> tc) {
    return append(0, key, val, tc, null);
  }

  @Override
  public OperationFuture<CASResponse> append(long cas, String key, Object val) {
    return append(cas, key, val, client.getTranscoder(), null);
  }

  @Override
  public <T> OperationFuture<CASResponse> append(long cas, String key, T val, Transcoder<T> tc) {
    throw new UnsupportedOperationException(); // TODO: clent.asyncCat(groupNode, ConcatenationType.append, cas, key, val, tc, null);
  }

  @Override
  public <T> OperationFuture<CASResponse> append(long cas, String key, T val, Transcoder<T> tc, OperationListener<CASResponse> listener) {
    throw new UnsupportedOperationException();
    // TODO: clent.asyncCat(groupNode, ConcatenationType.append, cas, key, val, tc, listener);
  }

  @Override
  public Future<CASResponse> prepend(String key, Object val) {
    return prepend(0, key, val, client.getTranscoder(), null);
  }

  @Override
  public <T> Future<CASResponse> prepend(String key, T val, Transcoder<T> tc) {
    return prepend(0, key, val, tc, null);
  }

  @Override
  public OperationFuture<CASResponse> prepend(long cas, String key, Object val) {
    return prepend(cas, key, val, client.getTranscoder(), null);
  }

  @Override
  public <T> OperationFuture<CASResponse> prepend(long cas, String key, T val, Transcoder<T> tc) {
    throw new UnsupportedOperationException();
    // TODO: clent.asyncCat(groupNode, ConcatenationType.prepend, cas, key, val, tc, null);
  }

  @Override
  public <T> OperationFuture<CASResponse> prepend(long cas, String key, T val, Transcoder<T> tc, OperationListener<CASResponse> listener) {
    throw new UnsupportedOperationException();
    // TODO: clent.asyncCat(groupNode, ConcatenationType.prepend, cas, key, val, tc, listener);
  }

  @Override
  public <T> Future<CASResponse> asyncCAS(String key, long casId, T value, Transcoder<T> tc, OperationListener<CASResponse> listener) {
    return asyncCAS(key, casId, 0, value, tc, listener);
  }

  @Override
  public <T> Future<CASResponse> asyncCAS(String key, long casId, T value, Transcoder<T> tc) {
    return asyncCAS(key, casId, 0, value, tc, null);
  }

  @Override
  public <T> Future<CASResponse> asyncCAS(String key, long casId, int exp,
                                          T value, Transcoder<T> tc) {
    return asyncCAS(key, casId, exp, value, tc, null);
  }

  @Override
  public Future<CASResponse> asyncCAS(String key, long casId, Object value) {
    return asyncCAS(key, casId, value, client.getTranscoder());
  }

  @Override
  public <T> CASResponse cas(String key, long casId, T value,
                             Transcoder<T> tc) {
    return cas(key, casId, 0, value, tc);
  }

  @Override
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

  @Override
  public CASResponse cas(String key, long casId, int exp, Object value) {
    return cas(key, casId, exp, value, client.getTranscoder());
  }

  @Override
  public CASResponse cas(String key, long casId, Object value) {
    return cas(key, casId, 0, value);
  }

  @Override
  public <T> OperationFuture<CASResponse> add(String key, int exp, T o,
      Transcoder<T> tc, OperationListener<CASResponse> listener) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncStore(groupNode, StoreType.add, key, exp, o, tc, listener);
  }

  @Override
  public <T> OperationFuture<CASResponse> add(String key, int exp, T o,
      Transcoder<T> tc) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncStore(groupNode, StoreType.add, key, exp, o, tc, null);
  }

  @Override
  public OperationFuture<CASResponse> add(String key, int exp, Object o) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncStore(groupNode, StoreType.add, key, exp, o, client.getTranscoder(), null);
  }

  @Override
  public <T> OperationFuture<CASResponse> set(String key, int exp, T o,
      Transcoder<T> tc, OperationListener<CASResponse> listener) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncStore(groupNode, StoreType.set, key, exp, o, tc, listener);
  }

  @Override
  public <T> OperationFuture<CASResponse> set(String key, int exp, T o,
      Transcoder<T> tc) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncStore(groupNode, StoreType.set, key, exp, o, tc, null);
  }

  @Override
  public OperationFuture<CASResponse> set(String key, int exp, Object o) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncStore(groupNode, StoreType.set, key, exp, o, client.getTranscoder(), null);
  }

  @Override
  public <T> OperationFuture<CASResponse> replace(String key, int exp, T o,
      Transcoder<T> tc, OperationListener<CASResponse> listener) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncStore(groupNode, StoreType.replace, key, exp, o, tc, listener);
  }

  @Override
  public <T> OperationFuture<CASResponse> replace(String key, int exp, T o,
      Transcoder<T> tc) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncStore(groupNode, StoreType.replace, key, exp, o, tc, null);
  }

  @Override
  public OperationFuture<CASResponse> replace(String key, int exp, Object o) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncStore(groupNode, StoreType.replace, key, exp, o, client.getTranscoder(), null);
  }

  @Override
  public <T> GetFuture<T> asyncGet(final String key, final Transcoder<T> tc, final OperationListener<T> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> GetFuture<T> asyncGet(final String key, final Transcoder<T> tc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public GetFuture<Object> asyncGet(final String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> OperationFuture<CASValue<T>> asyncGets(final String key,
      final Transcoder<T> tc, final OperationListener<CASValue<T>> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> OperationFuture<CASValue<T>> asyncGets(final String key,
      final Transcoder<T> tc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OperationFuture<CASValue<Object>> asyncGets(final String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> CASValue<T> gets(String key, Transcoder<T> tc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> CASValue<T> getAndTouch(String key, int exp, Transcoder<T> tc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CASValue<Object> getAndTouch(String key, int exp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CASValue<Object> gets(String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T get(String key, Transcoder<T> tc) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Object get(String key) {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
      Iterator<Transcoder<T>> tcIter, final OperationListener<Map<String, T>> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter,
      Iterator<Transcoder<T>> tcIter, final OperationListener<Map<String, CASValue<T>>> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter,
                                                                Iterator<Transcoder<T>> tcIter) {
    return asyncGetsBulk(keyIter, tcIter, null);
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
                                                                Iterator<Transcoder<T>> tcIter, OperationListener<Map<String, CASValue<T>>> listener) {
    return asyncGetsBulk(keys.iterator(), tcIter, listener);
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
                                                                Iterator<Transcoder<T>> tcIter) {
    return asyncGetsBulk(keys.iterator(), tcIter, null);
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter,
                                                                Transcoder<T> tc, OperationListener<Map<String, CASValue<T>>> listener) {
    return asyncGetsBulk(keyIter, new SingleElementInfiniteIterator<Transcoder<T>>(tc), listener);
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Iterator<String> keyIter,
                                                                Transcoder<T> tc) {
    return asyncGetsBulk(keyIter,
            new SingleElementInfiniteIterator<Transcoder<T>>(tc));
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
                                                                Transcoder<T> tc, OperationListener<Map<String, CASValue<T>>> listener) {
    return asyncGetsBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), listener);
  }

  @Override
  public <T> BulkFuture<Map<String, CASValue<T>>> asyncGetsBulk(Collection<String> keys,
                                                                Transcoder<T> tc) {
    return asyncGetsBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), null);
  }

  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(
          Iterator<String> keyIter, OperationListener<Map<String, CASValue<Object>>> listener) {
    return asyncGetsBulk(keyIter, client.getTranscoder(), listener);
  }

  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(
          Iterator<String> keyIter) {
    return asyncGetsBulk(keyIter, client.getTranscoder());
  }

  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(Collection<String> keys, OperationListener<Map<String, CASValue<Object>>> listener) {
    return asyncGetsBulk(keys, client.getTranscoder(), listener);
  }

  @Override
  public BulkFuture<Map<String, CASValue<Object>>> asyncGetsBulk(Collection<String> keys) {
    return asyncGetsBulk(keys, client.getTranscoder());
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
                                                     Iterator<Transcoder<T>> tcIter) {
    return asyncGetBulk(keyIter, tcIter, null);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
                                                     Iterator<Transcoder<T>> tcIter, OperationListener<Map<String, T>> listener) {
    return asyncGetBulk(keys.iterator(), tcIter, listener);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
                                                     Iterator<Transcoder<T>> tcIter) {
    return asyncGetBulk(keys.iterator(), tcIter, null);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
                                                     Transcoder<T> tc, OperationListener<Map<String, T>> listener) {
    return asyncGetBulk(keyIter, new SingleElementInfiniteIterator<Transcoder<T>>(tc), listener);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Iterator<String> keyIter,
                                                     Transcoder<T> tc) {
    return asyncGetBulk(keyIter,
            new SingleElementInfiniteIterator<Transcoder<T>>(tc));
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
                                                     Transcoder<T> tc, OperationListener<Map<String, T>> listener) {
    return asyncGetBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), listener);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Collection<String> keys,
                                                     Transcoder<T> tc) {
    return asyncGetBulk(keys, new SingleElementInfiniteIterator<Transcoder<T>>(
            tc), null);
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(
          Iterator<String> keyIter, OperationListener<Map<String, Object>> listener) {
    return asyncGetBulk(keyIter, client.getTranscoder(), listener);
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(
          Iterator<String> keyIter) {
    return asyncGetBulk(keyIter, client.getTranscoder());
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys, OperationListener<Map<String, Object>> listener) {
    return asyncGetBulk(keys, client.getTranscoder(), listener);
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(Collection<String> keys) {
    return asyncGetBulk(keys, client.getTranscoder());
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(OperationListener<Map<String, T>> listener, Transcoder<T> tc, String... keys) {
    return asyncGetBulk(Arrays.asList(keys), tc, listener);
  }

  @Override
  public <T> BulkFuture<Map<String, T>> asyncGetBulk(Transcoder<T> tc,
                                                     String... keys) {
    return asyncGetBulk(Arrays.asList(keys), tc);
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(OperationListener<Map<String, Object>> listener, String... keys) {
    return asyncGetBulk(Arrays.asList(keys), client.getTranscoder(), listener);
  }

  @Override
  public BulkFuture<Map<String, Object>> asyncGetBulk(String... keys) {
    return asyncGetBulk(Arrays.asList(keys), client.getTranscoder());
  }

  @Override
  public OperationFuture<CASValue<Object>> asyncGetAndTouch(final String key,
                                                            final int exp) {
    return asyncGetAndTouch(key, exp, client.getTranscoder());
  }

  @Override
  public <T> OperationFuture<CASValue<T>> asyncGetAndTouch(final String key,
                                                           final int exp, final Transcoder<T> tc) {
    return asyncGetAndTouch(key, exp, tc, null);
  }

  @Override
  public <T> OperationFuture<CASValue<T>> asyncGetAndTouch(final String key,
                                                           final int exp, final Transcoder<T> tc, final OperationListener<CASValue<T>> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
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

  @Override
  public Map<String, Object> getBulk(Iterator<String> keyIter) {
    return getBulk(keyIter, client.getTranscoder());
  }

  @Override
  public <T> Map<String, T> getBulk(Collection<String> keys,
                                    Transcoder<T> tc) {
    return getBulk(keys.iterator(), tc);
  }

  @Override
  public Map<String, Object> getBulk(Collection<String> keys) {
    return getBulk(keys, client.getTranscoder());
  }

  @Override
  public <T> Map<String, T> getBulk(Transcoder<T> tc, String... keys) {
    return getBulk(Arrays.asList(keys), tc);
  }

  @Override
  public Map<String, Object> getBulk(String... keys) {
    return getBulk(Arrays.asList(keys), client.getTranscoder());
  }

  @Override
  public Map<SocketAddress, String> getVersions() {
    return client.getVersions();
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats() {
    return getStats(null);
  }

  @Override
  public Map<SocketAddress, Map<String, String>> getStats(final String arg) {
    return client.getStats(arg);
  }

  @Override
  public long incr(String key, long by) {
    throw new UnsupportedOperationException();     // TODO: clent.mutate(groupNode, Mutator.incr, key, by, 0, -1);
  }

  @Override
  public long incr(String key, int by) {
    throw new UnsupportedOperationException();     // TODO: clent.mutate(groupNode, Mutator.incr, key, (long) by, 0, -1);
  }

  @Override
  public long decr(String key, long by) {
    throw new UnsupportedOperationException();     // TODO: clent.mutate(groupNode, Mutator.decr, key, by, 0, -1);
  }

  @Override
  public long decr(String key, int by) {
    throw new UnsupportedOperationException();     // TODO: clent.mutate(groupNode, Mutator.decr, key, (long) by, 0, -1);
  }

  @Override
  public long incr(String key, long by, long def, int exp) {
    throw new UnsupportedOperationException();     // TODO: clent.mutateWithDefault(groupNode, Mutator.incr, key, by, def, exp);
  }

  @Override
  public long incr(String key, int by, long def, int exp) {
    throw new UnsupportedOperationException();     // TODO: clent.mutateWithDefault(groupNode, Mutator.incr, key, (long) by, def, exp);
  }

  @Override
  public long decr(String key, long by, long def, int exp) {
    throw new UnsupportedOperationException();     // TODO: clent.mutateWithDefault(groupNode, Mutator.decr, key, by, def, exp);
  }

  @Override
  public long decr(String key, int by, long def, int exp) {
    throw new UnsupportedOperationException();     // TODO: clent.mutateWithDefault(groupNode, Mutator.decr, key, (long) by, def, exp);
  }

  @Override
  public OperationFuture<CASLongResponse> asyncIncr(String key, long by, OperationListener<CASLongResponse> listener) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncMutate(groupNode, Mutator.incr, key, by, 0, -1, listener);
  }

  @Override
  public OperationFuture<CASLongResponse> asyncIncr(String key, int by, OperationListener<CASLongResponse> listener) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncMutate(groupNode, Mutator.incr, key, (long) by, 0, -1, listener);
  }

  @Override
  public OperationFuture<CASLongResponse> asyncDecr(String key, long by, OperationListener<CASLongResponse> listener) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncMutate(groupNode, Mutator.decr, key, by, 0, -1, listener);
  }

  @Override
  public OperationFuture<CASLongResponse> asyncDecr(String key, int by, OperationListener<CASLongResponse> listener) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncMutate(groupNode, Mutator.decr, key, (long) by, 0, -1, listener);
  }

  @Override
  public OperationFuture<CASLongResponse> asyncIncr(String key, long by) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncMutate(groupNode, Mutator.incr, key, by, 0, -1, null);
  }

  @Override
  public OperationFuture<CASLongResponse> asyncIncr(String key, int by) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncMutate(groupNode, Mutator.incr, key, (long) by, 0, -1, null);
  }

  @Override
  public OperationFuture<CASLongResponse> asyncDecr(String key, long by) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncMutate(groupNode, Mutator.decr, key, by, 0, -1, null);
  }

  @Override
  public OperationFuture<CASLongResponse> asyncDecr(String key, int by) {
    throw new UnsupportedOperationException();     // TODO: clent.asyncMutate(groupNode, Mutator.decr, key, (long) by, 0, -1, null);
  }

  @Override
  public long incr(String key, long by, long def) {
    throw new UnsupportedOperationException();     // TODO: clent.mutateWithDefault(groupNode, Mutator.incr, key, by, def, 0);
  }

  @Override
  public long incr(String key, int by, long def) {
    throw new UnsupportedOperationException();     // TODO: clent.mutateWithDefault(groupNode, Mutator.incr, key, (long) by, def, 0);
  }

  @Override
  public long decr(String key, long by, long def) {
    throw new UnsupportedOperationException();     // TODO: clent.mutateWithDefault(groupNode, Mutator.decr, key, by, def, 0);
  }

  @Override
  public long decr(String key, int by, long def) {
    throw new UnsupportedOperationException();     // TODO: clent.mutateWithDefault(groupNode, Mutator.decr, key, (long) by, def, 0);
  }

  @Override
  public Future<CASResponse> delete(String key) {
    return client.delete(this, groupNode, true, key, 0, null);
  }

  @Override
  public Future<CASResponse> delete(String key, long cas) {
    return client.delete(this, groupNode, true, key, cas, null);
  }

  @Override
  public Future<CASResponse> delete(String key, final OperationListener<CASResponse> listener) {
    return client.delete(this, groupNode, true, key, 0, listener);
  }

  @Override
  public OperationFuture<Boolean> flush(final int delay) {
    throw new UnsupportedOperationException();     // TODO: clent.flush(delay);
  }

  @Override
  public OperationFuture<Boolean> flush() {
    throw new UnsupportedOperationException();     // TODO: clent.flush();
  }

  @Override
  public Set<String> listSaslMechanisms() {
    return client.listSaslMechanisms();
  }

  @Override
  public void shutdown() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean shutdown(long timeout, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean waitForQueues(long timeout, TimeUnit unit) {
    return client.waitForQueues(timeout, unit);
  }

  @Override
  public boolean addObserver(ConnectionObserver obs) {
    return client.addObserver(obs);
  }

  @Override
  public boolean removeObserver(ConnectionObserver obs) {
    return client.removeObserver(obs);
  }
}
