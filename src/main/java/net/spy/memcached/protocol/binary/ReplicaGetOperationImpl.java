/**
 * Copyright (C) 2009-2013 Couchbase, Inc.
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

import net.spy.memcached.ops.DataCallback;
import net.spy.memcached.ops.OperationStatus;
import net.spy.memcached.ops.ReplicaGetOperation;

/**
 * Implementation of the replica get operation.
 */
public class ReplicaGetOperationImpl extends SingleGetOperationImpl
  implements ReplicaGetOperation {

  static final byte GET_CMD = (byte)0x83;

  private final int replicaIndex;

  public ReplicaGetOperationImpl(String k, int index,
    DataCallback cb) {
    super(GET_CMD, generateOpaque(), k, cb);
    replicaIndex = index;
  }

  @Override
  protected void decodePayload(byte[] pl) {
    final int flags = decodeInt(pl, 0);
    final byte[] data = new byte[pl.length - EXTRA_HDR_LEN - keyLen];
    System.arraycopy(pl, (EXTRA_HDR_LEN + keyLen), data, 0,
        pl.length - EXTRA_HDR_LEN - keyLen);
    DataCallback gcb = (DataCallback) getCallback();
    try {
      gcb.gotData(key, flags, responseCas, data);
      gcb.receivedStatus(this, STATUS_OK);
    } catch (Throwable e) {
      gcb.receivedStatus(this, new OperationStatus(false, e.toString()));
    }
  }

  public int getReplicaIndex() {
    return replicaIndex;
  }
}
