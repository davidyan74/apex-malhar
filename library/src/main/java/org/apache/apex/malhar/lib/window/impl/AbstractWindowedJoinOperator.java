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

import java.util.Collection;
import java.util.Collections;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.JoinAccumulation;
import org.apache.apex.malhar.lib.window.JoinWindowedOperator;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.WindowedStorage;

import com.google.common.base.Function;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;


/**
 * Abstract Windowed Join Operator.
 */
public abstract class AbstractWindowedJoinOperator<InputT1, InputT2, OutputT, DataStorageT extends WindowedStorage,
    RetractionStorageT extends WindowedStorage, AccumulationT extends
    JoinAccumulation>
    extends AbstractWindowedOperator<InputT1, OutputT, DataStorageT, RetractionStorageT, AccumulationT>
    implements JoinWindowedOperator<InputT1, InputT2>
{

  private static final transient Logger LOG = LoggerFactory.getLogger(AbstractWindowedJoinOperator.class);
  private Function<InputT2, Long> timestampExtractor2;

  private long latestWatermark1 = -1;  // latest watermark from stream 1
  private long latestWatermark2 = -1;  // latest watermark from stream 2


  private static final transient Collection<? extends Window> GLOBAL_WINDOW_SINGLETON_SET = Collections.singleton(Window.GlobalWindow.getInstance());

  public final transient DefaultInputPort<Tuple<InputT2>> input2 = new DefaultInputPort<Tuple<InputT2>>()
  {
    @Override
    public void process(Tuple<InputT2> tuple)
    {
      processTuple2(tuple);
    }
  };

  // TODO: This port should be removed when Apex Core has native support for custom control tuples
  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<ControlTuple> controlInput2 = new DefaultInputPort<ControlTuple>()
  {
    @Override
    public void process(ControlTuple tuple)
    {
      if (tuple instanceof ControlTuple.Watermark) {
        processWatermark2((ControlTuple.Watermark)tuple);
      }
    }
  };

  public void processTuple2(Tuple<InputT2> tuple)
  {
    long timestamp = extractTimestamp2(tuple);
    if (isTooLate(timestamp)) {
      dropTuple2(tuple);
    } else {
      Tuple.WindowedTuple<InputT2> windowedTuple = getWindowedValue2(tuple);
      // do the accumulation
      accumulateTuple2(windowedTuple);

      for (Window window : windowedTuple.getWindows()) {
        WindowState windowState = windowStateMap.get(window);
        windowState.tupleCount++;
        // process any count based triggers
        if (windowState.watermarkArrivalTime == -1) {
          // watermark has not arrived yet, check for early count based trigger
          if (earlyTriggerCount > 0 && (windowState.tupleCount % earlyTriggerCount) == 0) {
            fireTrigger(window, windowState);
          }
        } else {
          // watermark has arrived, check for late count based trigger
          if (lateTriggerCount > 0 && (windowState.tupleCount % lateTriggerCount) == 0) {
            fireTrigger(window, windowState);
          }
        }
      }
    }
  }

  private long extractTimestamp2(Tuple<InputT2> tuple)
  {
    if (timestampExtractor2 == null) {
      if (tuple instanceof Tuple.TimestampedTuple) {
        return ((Tuple.TimestampedTuple)tuple).getTimestamp();
      } else {
        return 0;
      }
    } else {
      return timestampExtractor2.apply(tuple.getValue());
    }
  }

  public void dropTuple2(Tuple<InputT2> input)
  {
    // do nothing
    LOG.debug("Dropping late tuple {}", input);
  }

  public Tuple.WindowedTuple<InputT2> getWindowedValue2(Tuple<InputT2> input)
  {
    // windowOption can be null here, but the two upstream windowed operators must have same window option.
    // TODO: Perform window option check during validation. 
    if (windowOption == null && input instanceof Tuple.WindowedTuple) {
      // inherit the windows from upstream
      for (Window window : ((Tuple.WindowedTuple<InputT2>)input).getWindows()) {
        if (!windowStateMap.containsWindow(window)) {
          windowStateMap.put(window, new WindowState());
        }
      }
      return (Tuple.WindowedTuple<InputT2>)input;
    } else {
      return new Tuple.WindowedTuple<>(assignWindows2(input), extractTimestamp2(input), input.getValue());
    }
  }

  private Collection<? extends Window> assignWindows2(Tuple<InputT2> inputTuple)
  {
    if (windowOption instanceof WindowOption.GlobalWindow) {
      return GLOBAL_WINDOW_SINGLETON_SET;
    } else {
      long timestamp = extractTimestamp2(inputTuple);
      if (windowOption instanceof WindowOption.TimeWindows) {
        Collection<? extends Window> windows = getTimeWindowsForTimestamp(timestamp);
        for (Window window : windows) {
          if (!windowStateMap.containsWindow(window)) {
            windowStateMap.put(window, new WindowState());
          }
        }
        return windows;
      } else if (windowOption instanceof WindowOption.SessionWindows) {
        return assignSessionWindows2(timestamp, inputTuple);
      } else {
        throw new IllegalStateException("Unsupported Window Option: " + windowOption.getClass());
      }
    }
  }

  public void setTimestampExtractor2(Function<InputT2, Long> timestampExtractor2)
  {
    this.timestampExtractor2 = timestampExtractor2;
  }

  protected Collection<Window.SessionWindow> assignSessionWindows2(long timestamp, Tuple<InputT2> inputTuple)
  {
    throw new UnsupportedOperationException("Session window require keyed tuples");
  }


  @Override
  public void processWatermark(ControlTuple.Watermark watermark)
  {
    latestWatermark1 = watermark.getTimestamp();
    if (this.watermarkTimestamp < 0) {
      this.watermarkTimestamp = watermark.getTimestamp();
    } else {
      // Select the smallest timestamp of the latest watermarks as the watermark of the operator.
      if (Math.min(latestWatermark1, latestWatermark2) < this.watermarkTimestamp) {
        this.watermarkTimestamp = Math.min(latestWatermark1, latestWatermark2);
      }
    }
  }

  @Override
  public void processWatermark2(ControlTuple.Watermark watermark)
  {
    latestWatermark2 = watermark.getTimestamp();
    if (this.watermarkTimestamp < 0) {
      this.watermarkTimestamp = watermark.getTimestamp();
    } else {
      if (Math.min(latestWatermark1, latestWatermark2) < this.watermarkTimestamp) {
        this.watermarkTimestamp = Math.min(latestWatermark1, latestWatermark2);
      }
    }
  }

}
