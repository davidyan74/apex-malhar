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

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import org.apache.hadoop.classification.InterfaceStability;
import org.joda.time.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * This class describes how triggers should be fired for each window.
 * For each window, a trigger can be fired before the watermark (EARLY), at the watermark (ON_TIME), or after the watermark (LATE).
 * If a LATE trigger is specified and the accumulation mode is ACCUMULATING, it is important for the WindowOption to
 * specify the allowed lateness because otherwise, all states must be kept in storage.
 *
 */
@InterfaceStability.Evolving
public class TriggerOption
{
  public enum AccumulationMode
  {
    DISCARDING,
    ACCUMULATING,
    ACCUMULATING_AND_RETRACTING
  }

  private AccumulationMode accumulationMode = AccumulationMode.DISCARDING;

  /**
   * Whether the trigger should be fired before the watermark, at the watermark, or after the watermark
   */
  public enum WatermarkOpt
  {
    EARLY,
    ON_TIME,
    LATE
  }

  public static class Trigger
  {
    protected WatermarkOpt watermarkOpt;

    Trigger()
    {
      // for kryo
    }

    Trigger(WatermarkOpt watermarkOpt)
    {
      this.watermarkOpt = watermarkOpt;
    }

    public WatermarkOpt getWatermarkOpt()
    {
      return watermarkOpt;
    }
  }

  public static class TimeTrigger extends Trigger
  {
    @FieldSerializer.Bind(JavaSerializer.class)
    Duration duration;

    public TimeTrigger()
    {
      // for kryo
    }

    public TimeTrigger(WatermarkOpt watermarkOpt, Duration duration)
    {
      super(watermarkOpt);
      this.duration = duration;
    }

    public Duration getDuration()
    {
      return duration;
    }
  }

  public static class CountTrigger extends Trigger
  {
    long count;
    public CountTrigger()
    {
      //for kryo
    }

    public CountTrigger(WatermarkOpt watermarkOpt, long count)
    {
      super(watermarkOpt);
      this.count = count;
    }

    public long getCount()
    {
      return count;
    }
  }

  List<Trigger> triggerList = new ArrayList<>();

  /**
   * Creates a TriggerOption with an initial trigger that should be fired at the watermark
   *
   * @return
   */
  public static TriggerOption AtWatermark()
  {
    TriggerOption triggerOption = new TriggerOption();
    Trigger trigger = new Trigger(WatermarkOpt.ON_TIME);
    triggerOption.triggerList.add(trigger);
    return triggerOption;
  }

  /**
   * A trigger should be fired before the watermark once for every specified duration
   *
   * @param duration
   * @return
   */
  public TriggerOption withEarlyFiringsAtEvery(Duration duration)
  {
    TimeTrigger trigger = new TimeTrigger(WatermarkOpt.EARLY, duration);
    triggerList.add(trigger);
    return this;
  }

  /**
   * A trigger should be fired before the watermark once for every n tuple(s)
   *
   * @param count
   * @return
   */
  public TriggerOption withEarlyFiringsAtEvery(long count)
  {
    CountTrigger trigger = new CountTrigger(WatermarkOpt.EARLY, count);
    triggerList.add(trigger);
    return this;
  }

  /**
   * A trigger should be fired after the watermark once for every specified duration
   *
   * @param duration
   * @return
   */
  public TriggerOption withLateFiringsAtEvery(Duration duration)
  {
    TimeTrigger trigger = new TimeTrigger(WatermarkOpt.LATE, duration);
    triggerList.add(trigger);
    return this;
  }

  /**
   * A trigger should be fired after the watermark once for every n late tuple(s)
   *
   * @param count
   * @return
   */
  public TriggerOption withLateFiringsAtEvery(long count)
  {
    CountTrigger trigger = new CountTrigger(WatermarkOpt.LATE, count);
    triggerList.add(trigger);
    return this;
  }

  public TriggerOption discardingFiredPanes()
  {
    this.accumulationMode = AccumulationMode.DISCARDING;
    return this;
  }

  public TriggerOption accumulatingFiredPanes()
  {
    this.accumulationMode = AccumulationMode.ACCUMULATING;
    return this;
  }

  public TriggerOption accumulatingAndRetractingFiredPane()
  {
    this.accumulationMode = AccumulationMode.ACCUMULATING_AND_RETRACTING;
    return this;
  }

  public AccumulationMode getAccumulationMode()
  {
    return accumulationMode;
  }

  /**
   * Gets the trigger list
   *
   * @return
   */
  public List<Trigger> getTriggerList()
  {
    return Collections.unmodifiableList(triggerList);
  }
}
