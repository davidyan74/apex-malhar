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
package org.apache.apex.malhar.lib.window.impl;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponent;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeKryoSlice;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Function;
import com.google.common.collect.PeekingIterator;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * Implementation of WindowedKeyedStorage using {@link Spillable} data structures
 *
 * @param <K> The key type
 * @param <V> The value type
 */
public class SpillableWindowedKeyedStorage<K, V> implements WindowedStorage.WindowedKeyedStorage<K, V>
{
  @NotNull
  protected SpillableComplexComponent scc;
  protected long bucket;
  protected Serde<Window, Slice> windowSerde;
  protected Serde<Pair<Window, K>, Slice> windowKeyPairSerde;
  protected Serde<K, Slice> keySerde;
  protected Serde<V, Slice> valueSerde;
  protected long millisPerBucket;

  protected Spillable.SpillableIterableByteMap<Pair<Window, K>, V> windowKeyToValueMap;

  private static final Logger LOG = LoggerFactory.getLogger(SpillableWindowedKeyedStorage.class);

  private class KVIterator implements PeekingIterator<Map.Entry<K, V>>
  {
    final Window window;
    PeekingIterator<Map.Entry<Pair<Window, K>, V>> iterator;

    KVIterator(Window window)
    {
      this.window = window;
      this.iterator = windowKeyToValueMap.iterator(new ImmutablePair<>(window, (K)null));
    }

    @Override
    public boolean hasNext()
    {
      if (iterator != null && iterator.hasNext()) {
        Map.Entry<Pair<Window, K>, V> next = iterator.peek();
        return window.equals(next.getKey().getLeft());
      } else {
        return false;
      }
    }

    @Override
    public Map.Entry<K, V> peek()
    {
      Map.Entry<Pair<Window, K>, V> next = iterator.peek();
      if (window.equals(next.getKey().getLeft())) {
        return new AbstractMap.SimpleEntry<>(next.getKey().getRight(), next.getValue());
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public Map.Entry<K, V> next()
    {
      Map.Entry<Pair<Window, K>, V> next = iterator.peek();
      if (window.equals(next.getKey().getLeft())) {
        iterator.next();
        return new AbstractMap.SimpleEntry<>(next.getKey().getRight(), next.getValue());
      } else {
        throw new NoSuchElementException();
      }
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }

  public SpillableWindowedKeyedStorage()
  {
  }

  public SpillableWindowedKeyedStorage(long bucket,
      Serde<Window, Slice> windowSerde, Serde<Pair<Window, K>, Slice> windowKeyPairSerde, Serde<K, Slice> keySerde, Serde<V, Slice> valueSerde, long millisPerBucket)
  {
    this.bucket = bucket;
    this.windowSerde = windowSerde;
    this.windowKeyPairSerde = windowKeyPairSerde;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
    this.millisPerBucket = millisPerBucket;
  }

  public void setSpillableComplexComponent(SpillableComplexComponent scc)
  {
    this.scc = scc;
  }

  public void setBucket(long bucket)
  {
    this.bucket = bucket;
  }

  public void setWindowSerde(Serde<Window, Slice> windowSerde)
  {
    this.windowSerde = windowSerde;
  }

  public void setWindowKeyPairSerde(Serde<Pair<Window, K>, Slice> windowKeyPairSerde)
  {
    this.windowKeyPairSerde = windowKeyPairSerde;
  }

  public void setValueSerde(Serde<V, Slice> valueSerde)
  {
    this.valueSerde = valueSerde;
  }

  @Override
  public boolean containsWindow(Window window)
  {
    return windowKeyToValueMap.containsKey(new ImmutablePair<Window, K>(window, null));
  }

  @Override
  public void remove(Window window)
  {
    Iterator<Map.Entry<K, V>> it = iterator(window);
    while (it.hasNext()) {
      it.remove();
    }
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (bucket == 0) {
      // choose a bucket that is guaranteed to be unique in Apex
      bucket = (context.getValue(Context.DAGContext.APPLICATION_NAME) + "#" + context.getId()).hashCode();
    }
    // set default serdes
    if (windowSerde == null) {
      windowSerde = new SerdeKryoSlice<>();
    }
    if (windowKeyPairSerde == null) {
      windowKeyPairSerde = new SerdeKryoSlice<>();
    }
    if (keySerde == null) {
      keySerde = new SerdeKryoSlice<>();
    }
    if (valueSerde == null) {
      valueSerde = new SerdeKryoSlice<>();
    }

    if (windowKeyToValueMap == null) {
      windowKeyToValueMap = scc.newSpillableIterableByteMap(bucket, windowKeyPairSerde, valueSerde, new Function<Pair<Window, K>, Long>()

      {
        @Override
        public Long apply(@Nullable Pair<Window, K> windowKPair)
        {
          return windowKPair.getLeft().getBeginTimestamp();
        }
      }, millisPerBucket);
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void put(Window window, K key, V value)
  {
    windowKeyToValueMap.put(new ImmutablePair<>(window, key), value);
  }

  @Override
  public Iterator<Map.Entry<K, V>> iterator(final Window window)
  {
    return new KVIterator(window);
  }

  @Override
  public Iterable<Map.Entry<K, V>> entries(final Window window)
  {
    return new Iterable<Map.Entry<K, V>>()
    {
      @Override
      public Iterator<Map.Entry<K, V>> iterator()
      {
        return SpillableWindowedKeyedStorage.this.iterator(window);
      }
    };
  }

  @Override
  public V get(Window window, K key)
  {
    return windowKeyToValueMap.get(new ImmutablePair<>(window, key));
  }

}
