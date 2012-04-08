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

import net.spy.memcached.ops.OperationCallback;
import net.spy.memcached.ops.StoreType;

class QuietStoreOperationImpl extends StoreOperationImpl {
  static final byte SETQ = 0x11;
  static final byte ADDQ = 0x12;
  static final byte REPLACEQ = 0x13;

  private static byte cmdMap(StoreType t) {
    byte rv;
    switch (t) {
    case set:
      rv = SETQ;
      break;
    case add:
      rv = ADDQ;
      break;
    case replace:
      rv = REPLACEQ;
      break;
    default:
      rv = DUMMY_OPCODE;
    }
    // Check fall-through.
    assert rv != DUMMY_OPCODE : "Unhandled store type:  " + t;
    return rv;
  }

  public QuietStoreOperationImpl(StoreType t, String k,
      int f, int e, byte[] d, long c,
      OperationCallback cb) {
    super(t, cmdMap(t), k, f, e, d, c, cb);
  }

  @Override
  public boolean isQuiet() {
    return true;
  }
}
