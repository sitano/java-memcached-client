/**
 * Copyright (C) 2006-2009 Dustin Sallings
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

package net.spy.memcached.protocol;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import net.spy.memcached.ops.DataCallback;
import net.spy.memcached.ops.GetOperation;
import net.spy.memcached.ops.Operation;
import net.spy.memcached.ops.OperationStatus;

/**
 * Proxy callback used for dispatching callbacks over optimized gets.
 */
public class ProxyCallback implements DataCallback {

  private final Map<String, Collection<DataCallback>> callbacks =
      new HashMap<String, Collection<DataCallback>>();
  private final Collection<DataCallback> allCallbacks =
      new ArrayList<DataCallback>();

  public void addCallbacks(GetOperation o) {
    DataCallback c =
        new GetCallbackWrapper(o, o.getKeys().size(),
            (DataCallback) o.getCallback());
    allCallbacks.add(c);
    for (String s : o.getKeys()) {
      Collection<DataCallback> cbs = callbacks.get(s);
      if (cbs == null) {
        cbs = new ArrayList<DataCallback>();
        callbacks.put(s, cbs);
      }
      cbs.add(c);
    }
  }

  @Override
  public void gotData(String key, int flags, long cas, byte[] data) {
    Collection<DataCallback> cbs = callbacks.get(key);
    assert cbs != null : "No callbacks for key " + key;
    for (DataCallback c : cbs) {
      c.gotData(key, flags, cas, data);
    }
  }

  @Override
  public void receivedStatus(Operation operation, OperationStatus status) {
    for (DataCallback c : allCallbacks) {
      c.receivedStatus(operation, status);
    }
  }

  @Override
  public void complete(Operation operation) {
    for (DataCallback c : allCallbacks) {
      c.complete(operation);
    }
  }

  public int numKeys() {
    return callbacks.size();
  }

  public int numCallbacks() {
    return allCallbacks.size();
  }
}
