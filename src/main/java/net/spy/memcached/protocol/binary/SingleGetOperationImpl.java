package net.spy.memcached.protocol.binary;

import net.spy.memcached.ops.DataCallback;
import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.OperationStatus;

/**
 * Encapsulate same payload processing for binary key/value ops
 */
abstract public class SingleGetOperationImpl extends SingleKeyOperationImpl {
  /**
   * Length of the extra header stuff for a GET response.
   */
  static final int EXTRA_HDR_LEN = 4;

  protected SingleGetOperationImpl(byte c, int o, String k, OperationCallback cb) {
    super(c, o, k, cb);
  }

  @Override
  public void initialize() {
    prepareBuffer(key, 0, EMPTY_BYTES);
  }

  @Override
  protected void decodePayload(byte[] pl) {
    final int flags = decodeInt(pl, 0);
    final byte[] data = new byte[pl.length - EXTRA_HDR_LEN];
    System.arraycopy(pl, EXTRA_HDR_LEN, data, 0, pl.length - EXTRA_HDR_LEN);
    DataCallback gcb = (DataCallback) getCallback();
    try {
      gcb.gotData(key, flags, responseCas, data);
      gcb.receivedStatus(this, STATUS_OK);
    } catch (Throwable e) {
      gcb.receivedStatus(this, new OperationStatus(false, e.toString()));
    }
  }
}
