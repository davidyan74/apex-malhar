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

import java.util.Iterator;
import java.util.Map;

import javax.validation.constraints.NotNull;

import org.apache.apex.malhar.lib.state.spillable.Spillable;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponent;
import org.apache.apex.malhar.lib.utils.serde.Serde;
import org.apache.apex.malhar.lib.utils.serde.SerdeKryoSlice;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;

import com.datatorrent.api.Context;
import com.datatorrent.netlet.util.Slice;

/**
 * This is an implementation of WindowedPlainStorage that makes use of {@link Spillable} data structures
 *
 * @param <T> Type of the value per window
 */
public class SpillableWindowedPlainStorage<T> implements WindowedStorage.WindowedPlainStorage<T>
{
  @NotNull
  private SpillableComplexComponent scc;
  private long bucket;
  private Serde<Window, Slice> windowSerde;
  private Serde<T, Slice> valueSerde;

  protected Spillable.SpillableByteMap<Window, T> windowToDataMap;

  public SpillableWindowedPlainStorage()
  {
  }

  public SpillableWindowedPlainStorage(long bucket, Serde<Window, Slice> windowSerde, Serde<T, Slice> valueSerde)
  {
    this.bucket = bucket;
    this.windowSerde = windowSerde;
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

  public void setValueSerde(Serde<T, Slice> valueSerde)
  {
    this.valueSerde = valueSerde;
  }

  @Override
  public void put(Window window, T value)
  {
    windowToDataMap.put(window, value);
  }

  @Override
  public T get(Window window)
  {
    return windowToDataMap.get(window);
  }

  @Override
  public Iterable<Map.Entry<Window, T>> entrySet()
  {
    return windowToDataMap.entrySet();
  }

  @Override
  public Iterator<Map.Entry<Window, T>> iterator()
  {
    return entrySet().iterator();
  }

  @Override
  public boolean containsWindow(Window window)
  {
    return windowToDataMap.containsKey(window);
  }

  @Override
  public void remove(Window window)
  {
    windowToDataMap.remove(window);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    if (bucket == 0) {
      // choose a bucket that is almost guaranteed to be unique
      bucket = (context.getValue(Context.DAGContext.APPLICATION_NAME) + "#" + context.getId()).hashCode();
    }
    // set default serdes
    if (windowSerde == null) {
      windowSerde = new SerdeKryoSlice<>();
    }
    if (valueSerde == null) {
      valueSerde = new SerdeKryoSlice<>();
    }
    if (windowToDataMap == null) {
      windowToDataMap = scc.newSpillableByteMap(bucket, windowSerde, valueSerde);
    }
  }

  @Override
  public void teardown()
  {
  }

}
