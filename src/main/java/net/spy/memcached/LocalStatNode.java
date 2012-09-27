/**
 * Copyright (C) 2012 Ivan Prisyazhniy
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

import java.util.EnumMap;
import java.util.Map;

public class LocalStatNode {
  public final MemcachedNode node;
  public final Map<LocalStatType, String> stats;

  public LocalStatNode(MemcachedNode node) {
    this.node = node;
    this.stats = new EnumMap<LocalStatType, String>(LocalStatType.class);
  }

  public LocalStatNode(MemcachedNode node, Map<LocalStatType, String> stats) {
    this.node = node;
    this.stats = stats;
  }

  @Override
  public String toString() {
    return node + ": " + stats;
  }
}
