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

import net.spy.memcached.ops.ConcatenationType;
import net.spy.memcached.ops.OperationCallback;

class QuietConcatenationOperationImpl extends ConcatenationOperationImpl {
  private static final int APPENDQ = 0x19;
  private static final int PREPENDQ = 0x1a;

  private static byte cmdMap(ConcatenationType t) {
    byte rv;
    switch (t) {
    case append:
      rv = APPENDQ;
      break;
    case prepend:
      rv = PREPENDQ;
      break;
    default:
      rv = DUMMY_OPCODE;
    }
    // Check fall-through.
    assert rv != DUMMY_OPCODE : "Unhandled store type:  " + t;
    return rv;
  }

  public QuietConcatenationOperationImpl(ConcatenationType t,
      String k, byte[] d, long c,
      OperationCallback cb) {
    super(t, cmdMap(t), k, d, c, cb);
  }

  @Override
  public boolean isQuiet() {
    return true;
  }
}
