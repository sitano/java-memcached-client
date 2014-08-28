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

package net.spy.memcached.protocol.binary;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import javax.security.auth.callback.CallbackHandler;

import net.spy.memcached.ops.*;
import net.spy.memcached.tapmessage.RequestMessage;
import net.spy.memcached.tapmessage.TapOpcode;

/**
 * Factory for binary operations.
 */
public class BinaryOperationFactory extends BaseOperationFactory {

  public DeleteOperation delete(String key, DataCallback operationCallback) {
    return new DeleteOperationImpl(key, operationCallback);
  }

  public DeleteOperation delete(String key, long cas, DataCallback operationCallback) {
    return new DeleteOperationImpl(key, cas, operationCallback);
  }

  public DeleteOperation deleteQuiet(String key) {
    return new QuietDeleteOperationImpl(key);
  }

  public DeleteOperation deleteQuiet(String key, long cas) {
    return new QuietDeleteOperationImpl(key, cas);
  }

  public UnlockOperation unlock(String key, long casId,
      OperationCallback cb) {
    return new UnlockOperationImpl(key, casId, cb);
  }
  public ObserveOperation observe(String key, long casId, int index,
          ObserveOperation.Callback cb) {
    return new ObserveOperationImpl(key, casId, index, cb);
  }
  public FlushOperation flush(int delay, OperationCallback cb) {
    return new FlushOperationImpl(cb);
  }

  public GetAndTouchOperation getAndTouch(String key, int expiration,
                                          DataCallback cb) {
    return new GetAndTouchOperationImpl(key, expiration, cb);
  }

  public GetOperation get(String key, DataCallback callback) {
    return new GetOperationImpl(key, callback);
  }

  public GetOperation get(Collection<String> value, DataCallback cb) {
    return new MultiGetOperationImpl(value, cb);
  }

  public ReplicaGetOperation replicaGet(String key, int index,
                                        DataCallback callback) {
    return new ReplicaGetOperationImpl(key, index, callback);
  }

  public GetlOperation getl(String key, int exp, DataCallback cb) {
    return new GetlOperationImpl(key, exp, cb);
  }

  public GetsOperation gets(String key, DataCallback cb) {
    return new GetsOperationImpl(key, cb);
  }

  public GetsOperation gets(Collection<String> keys, DataCallback cb) {
    return new MultiGetsOperationImpl(keys, cb);
  }

  public StatsOperation keyStats(String key, StatsOperation.Callback cb) {
    return new KeyStatsOperationImpl(key, cb);
  }

  public MutatorOperation mutate(Mutator m, String key, long by, long def,
      int exp, OperationCallback cb) {
    return new MutatorOperationImpl(m, key, by, def, exp, cb);
  }

  public StatsOperation stats(String arg,
      net.spy.memcached.ops.StatsOperation.Callback cb) {
    return new StatsOperationImpl(arg, cb);
  }

  public StoreOperation store(StoreType storeType, String key, int flags,
      int exp, byte[] data, OperationCallback cb) {
    return new StoreOperationImpl(storeType, key, flags, exp, data, 0, cb);
  }

  public KeyedOperation touch(String key, int expiration,
      OperationCallback cb) {
    return new TouchOperationImpl(key, expiration, cb);
  }

  public VersionOperation version(OperationCallback cb) {
    return new VersionOperationImpl(cb);
  }

  public NoopOperation noop(OperationCallback cb) {
    return new NoopOperationImpl(cb);
  }

  public CASOperation cas(StoreType type, String key, long casId, int flags,
      int exp, byte[] data, OperationCallback cb) {
    return new StoreOperationImpl(type, key, flags, exp, data, casId, cb);
  }

  public ConcatenationOperation cat(ConcatenationType catType, long casId,
      String key, byte[] data, OperationCallback cb) {
    return new ConcatenationOperationImpl(catType, key, data, casId, cb);
  }

  @Override
  protected Collection<? extends Operation> cloneGet(KeyedOperation op) {
    Collection<Operation> rv = new ArrayList<Operation>();
    if (op instanceof GetOperation) {
      DataCallback cb = new MultiGetOperationCallback(op.getCallback(), op.getKeys().size());
      for (String k : op.getKeys()) {
        rv.add(get(k, cb));
      }
    } else if(op instanceof ReplicaGetOperation) {
      DataCallback replicaGetCb = new MultiReplicaGetOperationCallback(op.getCallback(), op.getKeys().size());
      for (String k : op.getKeys()) {
        rv.add(replicaGet(k, ((ReplicaGetOperationImpl)op).getReplicaIndex() ,replicaGetCb));
      }
    } else {
      DataCallback cb = new MultiGetsOperationCallback(op.getCallback(), op.getKeys().size());
      for (String k : op.getKeys()) {
        rv.add(gets(k, cb));
      }
    }
    return rv;
  }

  public SASLAuthOperation saslAuth(String[] mech, String serverName,
      Map<String, ?> props, CallbackHandler cbh, OperationCallback cb) {
    return new SASLAuthOperationImpl(mech, serverName, props, cbh, cb);
  }

  public SASLMechsOperation saslMechs(OperationCallback cb) {
    return new SASLMechsOperationImpl(cb);
  }

  public SASLStepOperation saslStep(String[] mech, byte[] challenge,
      String serverName, Map<String, ?> props, CallbackHandler cbh,
      OperationCallback cb) {
    return new SASLStepOperationImpl(mech, challenge, serverName, props, cbh,
        cb);
  }

  public TapOperation tapBackfill(String id, long date, OperationCallback cb) {
    return new TapBackfillOperationImpl(id, date, cb);
  }

  public TapOperation tapBackfill(String id, Map<Short,Long> checkpointMap, OperationCallback cb) {
    return new TapBackfillOperationImpl(id, checkpointMap, cb);
  }

  public TapOperation tapCustom(String id, RequestMessage message, OperationCallback cb) {
    return new TapCustomOperationImpl(id, message, cb);
  }

  public TapOperation tapDeregister(String id, OperationCallback cb) {
    return new TapDeregisterOperationImpl(id, cb);
  }

  public TapOperation tapAck(TapOpcode opcode, int opaque, OperationCallback cb) {
    return new TapAckOperationImpl(opcode, opaque, cb);
  }

  public TapOperation tapDump(String id, OperationCallback cb) {
    return new TapDumpOperationImpl(id, cb);
  }
}
