package net.spy.memcached.protocol.binary;

import net.spy.memcached.ops.OperationCallback;

abstract public class SingleGetExpOperationImpl extends SingleGetOperationImpl {
  private final int exp;

  protected SingleGetExpOperationImpl(byte c, int o, String k, int e, OperationCallback cb) {
    super(c, o, k, cb);
    this.exp = e;
  }

  @Override
  public void initialize() {
    prepareBuffer(key, 0, EMPTY_BYTES, exp);
  }

  @Override
  public String toString() {
    return super.toString() + " Exp: " + exp;
  }
}
