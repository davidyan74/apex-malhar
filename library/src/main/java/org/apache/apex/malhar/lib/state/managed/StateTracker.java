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
package org.apache.apex.malhar.lib.state.managed;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;

import javax.validation.constraints.NotNull;

import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

/**
 * Tracks the size of state in memory and evicts buckets.
 */
class StateTracker extends TimerTask
{
  private final transient Timer memoryFreeService = new Timer();

  protected transient AbstractManagedStateImpl managedStateImpl;

  private transient long lastUpdateAccessTime = 0;
  private final transient Set<Long> accessedBucketIds = Sets.newHashSet();
  private transient TreeSet<BucketIdTimeWrapper> bucketHeapForAccess = Sets.newTreeSet();
  private transient TreeSet<BucketIdTimeWrapper> bucketHeapForRelease = Sets.newTreeSet();

  private int updateAccessTimeInterval = 500;

  void setup(@NotNull AbstractManagedStateImpl managedStateImpl)
  {
    this.managedStateImpl = Preconditions.checkNotNull(managedStateImpl, "managed state impl");

    long intervalMillis = managedStateImpl.getCheckStateSizeInterval().getMillis();
    memoryFreeService.scheduleAtFixedRate(this, intervalMillis, intervalMillis);
  }

  void bucketAccessed(long bucketId)
  {
    accessedBucketIds.add(bucketId);
    if (System.currentTimeMillis() - lastUpdateAccessTime > updateAccessTimeInterval) {
      synchronized (this) {
        //remove the duplicate
        Iterator<BucketIdTimeWrapper> bucketIter = bucketHeapForAccess.iterator();
        while (bucketIter.hasNext()) {
          if (accessedBucketIds.contains(bucketIter.next().bucketId)) {
            bucketIter.remove();
          }
        }

        for (long id : accessedBucketIds) {
          bucketHeapForAccess.add(new BucketIdTimeWrapper(id));
        }
      }

      accessedBucketIds.clear();
      lastUpdateAccessTime = System.currentTimeMillis();
    }
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  @Override
  public void run()
  {
    synchronized (managedStateImpl.commitLock) {
      //freeing of state needs to be stopped during commit as commit results in transferring data to a state which
      // can be freed up as well.
      long bytesSum = 0;
      for (Bucket bucket : managedStateImpl.buckets) {
        if (bucket != null) {
          bytesSum += bucket.getSizeInBytes();
        }
      }

      if (bytesSum > managedStateImpl.getMaxMemorySize()) {
        Duration duration = managedStateImpl.getDurationPreventingFreeingSpace();
        long durationMillis = 0;
        if (duration != null) {
          durationMillis = duration.getMillis();
        }

        synchronized (this) {
          /**
           * The buckets need to be free memory are the buckets accessed in this period and the buckets leftover from last time release.
           * As the leftover buckets should be very small size or empty.
           * So add leftover buckets to current accessed buckets and then swap should performance better.
           */
          //add the last release leftover for this release
          for (BucketIdTimeWrapper releaseItem : bucketHeapForRelease) {
            bucketHeapForAccess.add(releaseItem);
          }
          bucketHeapForRelease.clear();

          //switch the access and release list
          TreeSet<BucketIdTimeWrapper> tmp = bucketHeapForAccess;
          bucketHeapForAccess = bucketHeapForRelease;
          bucketHeapForRelease = tmp;
        }

        Iterator<BucketIdTimeWrapper> bucketIter = bucketHeapForRelease.iterator();
        BucketIdTimeWrapper idTimeWrapper;
        while (bytesSum > managedStateImpl.getMaxMemorySize() && bucketIter.hasNext()) {
          //trigger buckets to free space
          idTimeWrapper = bucketIter.next();

          if (System.currentTimeMillis() - idTimeWrapper.lastAccessedTime < durationMillis) {
            //if the least recently used bucket cannot free up space because it was accessed within the
            //specified duration then subsequent buckets cannot free space as well because this heap is ordered by time.
            break;
          }
          long bucketId = idTimeWrapper.bucketId;
          Bucket bucket = managedStateImpl.getBucket(bucketId);
          if (bucket != null) {

            synchronized (bucket) {
              long sizeFreed;
              try {
                sizeFreed = bucket.freeMemory(managedStateImpl.getCheckpointManager().getLastTransferredWindow());
                LOG.debug("bucket freed {} {}", bucketId, sizeFreed);
              } catch (IOException e) {
                managedStateImpl.throwable.set(e);
                throw new RuntimeException("freeing " + bucketId, e);
              }
              bytesSum -= sizeFreed;
            }
            if (bucket.getSizeInBytes() == 0) {
              bucketIter.remove();
            }
          }
        }
      }
    }
  }

  public int getUpdateAccessTimeInterval()
  {
    return updateAccessTimeInterval;
  }

  public void setUpdateAccessTimeInterval(int updateAccessTimeInterval)
  {
    this.updateAccessTimeInterval = updateAccessTimeInterval;
  }

  void teardown()
  {
    memoryFreeService.cancel();
  }

  /**
   * Wrapper class for bucket id and the last time the bucket was accessed.
   */
  private static class BucketIdTimeWrapper implements Comparable<BucketIdTimeWrapper>
  {
    @Override
    public int compareTo(BucketIdTimeWrapper o)
    {
      return (int)((lastAccessedTime == o.lastAccessedTime) ? (bucketId - o.bucketId) : lastAccessedTime - o.lastAccessedTime);
    }

    private final long bucketId;
    private long lastAccessedTime;

    BucketIdTimeWrapper(long bucketId)
    {
      this.bucketId = bucketId;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (!(o instanceof BucketIdTimeWrapper)) {
        return false;
      }

      BucketIdTimeWrapper that = (BucketIdTimeWrapper)o;
      //Note: the comparator used with bucket heap imposes orderings that are inconsistent with equals
      return bucketId == that.bucketId;

    }

    @Override
    public int hashCode()
    {
      return (int)(bucketId ^ (bucketId >>> 32));
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(StateTracker.class);

}
