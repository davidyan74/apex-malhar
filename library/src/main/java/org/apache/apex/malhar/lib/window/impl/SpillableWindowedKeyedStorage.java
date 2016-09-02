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
import java.util.List;
import java.util.Map;

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

  protected Spillable.SpillableByteMap<Pair<Window, K>, V> windowKeyToValueMap;
  protected Spillable.SpillableByteArrayListMultimap<Window, K> windowToKeysMap;

  private static final Logger LOG = LoggerFactory.getLogger(SpillableWindowedKeyedStorage.class);

  private class KVIterator implements Iterator<Map.Entry<K, V>>
  {
    final Window window;
    final List<K> keys;
    Iterator<K> iterator;

    KVIterator(Window window)
    {
      this.window = window;
      this.keys = windowToKeysMap.get(window);
      if (this.keys != null) {
        this.iterator = this.keys.iterator();
      }
    }

    @Override
    public boolean hasNext()
    {
      return iterator != null && iterator.hasNext();
    }

    @Override
    public Map.Entry<K, V> next()
    {
      K key = iterator.next();
      return new AbstractMap.SimpleEntry<>(key, windowKeyToValueMap.get(new ImmutablePair<>(window, key)));
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
      Serde<Window, Slice> windowSerde, Serde<Pair<Window, K>, Slice> windowKeyPairSerde, Serde<K, Slice> keySerde, Serde<V, Slice> valueSerde)
  {
    this.bucket = bucket;
    this.windowSerde = windowSerde;
    this.windowKeyPairSerde = windowKeyPairSerde;
    this.keySerde = keySerde;
    this.valueSerde = valueSerde;
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
    return windowToKeysMap.containsKey(window);
  }

  @Override
  public long size()
  {
    return windowToKeysMap.size();
  }

  @Override
  public void remove(Window window)
  {
    LOG.debug("before windowKeyToValueMap size {}", windowKeyToValueMap.size());
    LOG.debug("before windowKeyToValueMap size {}", windowToKeysMap.size());
    List<K> keys = windowToKeysMap.get(window);
    for (K key : keys) {
      windowKeyToValueMap.remove(new ImmutablePair<>(window, key));
    }
    LOG.debug("after windowKeyToValueMap size {}", windowKeyToValueMap.size());
    windowToKeysMap.removeAll(window);
    LOG.debug("after windowKeyToValueMap size {}", windowToKeysMap.size());
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
      windowKeyToValueMap = scc.newSpillableByteMap(bucket, windowKeyPairSerde, valueSerde);
    }
    if (windowToKeysMap == null) {
      windowToKeysMap = scc.newSpillableByteArrayListMultimap(bucket, windowSerde, keySerde);
    }
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void put(Window window, K key, V value)
  {
    if (!windowToKeysMap.containsEntry(window, key)) {
      windowToKeysMap.put(window, key);
    }
    windowKeyToValueMap.put(new ImmutablePair<>(window, key), value);
  }

  @Override
  public Iterable<Map.Entry<K, V>> entrySet(final Window window)
  {
    return new Iterable<Map.Entry<K, V>>()
    {
      @Override
      public Iterator<Map.Entry<K, V>> iterator()
      {
        return new KVIterator(window);
      }
    };
  }

  @Override
  public V get(Window window, K key)
  {
    return windowKeyToValueMap.get(new ImmutablePair<>(window, key));
  }

}
