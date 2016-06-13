/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.stream.window;

import org.apache.hadoop.classification.InterfaceStability;

import java.util.Comparator;

/**
 * This interface describes the individual window.
 */
@InterfaceStability.Evolving
public interface Window
{
  long getBeginTimestamp();
  long getDurationMillis();

  /**
   * Global window means there is only one window, or no window depending on how you look at it.
   */
  class GlobalWindow implements Window
  {
    @Override
    public long getBeginTimestamp()
    {
      return 0;
    }
    public long getDurationMillis()
    {
      return Long.MAX_VALUE;
    }
  }

  class DefaultComparator implements Comparator<Window>
  {
    @Override
    public int compare(Window o1, Window o2)
    {
      if (o1.getBeginTimestamp() < o2.getBeginTimestamp()) {
        return -1;
      } else if (o1.getBeginTimestamp() > o2.getBeginTimestamp()) {
        return 1;
      } else if (o1.getDurationMillis() < o2.getDurationMillis()) {
        return -1;
      } else if (o1.getDurationMillis() > o2.getDurationMillis()) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  GlobalWindow GLOBAL_WINDOW = new GlobalWindow();
  Comparator<Window> DEFAULT_COMPARATOR = new DefaultComparator();

  /**
   * TimeWindow is a window that represents a time slice
   */
  class TimeWindow implements Window
  {
    protected final long beginTimestamp;
    protected final long durationMillis;

    public TimeWindow(long beginTimestamp, long durationMillis)
    {
      this.beginTimestamp = beginTimestamp;
      this.durationMillis = durationMillis;
    }

    /**
     * Gets the beginning timestamp of this window
     *
     * @return
     */
    @Override
    public long getBeginTimestamp()
    {
      return beginTimestamp;
    }

    /**
     * Gets the duration millis of this window
     *
     * @return
     */
    @Override
    public long getDurationMillis()
    {
      return durationMillis;
    }

  }

  /**
   * SessionWindow is a window that represents a time slice for a key, with the time slice being variable length
   * @param <K>
   */
  class SessionWindow<K> extends TimeWindow
  {
    private final K key;

    public SessionWindow(K key, long beginTimestamp, long duration)
    {
      super(beginTimestamp, duration);
      this.key = key;
    }

    /**
     * Merges the two session windows and forms one window that spans the two windows.
     * The caller of this method is responsible for checking whether the two windows are close enough for merging
     * (i.e. calling {@link #shouldMerge})
     *
     * @param w1
     * @param w2
     * @param <K>
     * @return
     */
    public static <K> SessionWindow<K> merge(SessionWindow<K> w1, SessionWindow<K> w2)
    {
      if ((w1.key != null && w1.key != null) || !w1.key.equals(w2.key)) {
        throw new IllegalArgumentException("The keys of the two session windows do not match");
      }
      long beginTimestamp = Math.min(w1.beginTimestamp, w2.beginTimestamp);
      long endTimestamp = Math.max(w1.beginTimestamp + w1.durationMillis, w2.beginTimestamp + w2.durationMillis);
      return new SessionWindow<>(w1.key, beginTimestamp, endTimestamp - beginTimestamp);
    }

    /**
     * Given the two session windows and the minimum gap of session windows, determine whether
     * the two windows should be merged
     *
     * @param w1
     * @param w2
     * @param minGap
     * @param <K>
     * @return
     */
    public static <K> boolean shouldMerge(SessionWindow<K> w1, SessionWindow<K> w2, long minGap)
    {
      if (!((w1.key == null && w2.key == null) || w1.key.equals(w2.key))) {
        return false;
      }
      if (w1.beginTimestamp == w2.beginTimestamp) {
        return true;
      }
      if (w1.beginTimestamp < w2.beginTimestamp) {
        return (w1.beginTimestamp + w1.durationMillis + minGap > w2.beginTimestamp);
      } else {
        return (w2.beginTimestamp + w2.durationMillis + minGap > w1.beginTimestamp);
      }
    }
  }
}