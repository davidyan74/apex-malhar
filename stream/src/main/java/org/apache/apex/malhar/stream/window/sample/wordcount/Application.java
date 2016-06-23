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
package org.apache.apex.malhar.stream.window.sample.wordcount;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.util.KeyValPair;
import com.google.common.base.Throwables;
import org.apache.apex.malhar.stream.window.Accumulation;
import org.apache.apex.malhar.stream.window.TriggerOption;
import org.apache.apex.malhar.stream.window.Tuple;
import org.apache.apex.malhar.stream.window.WindowOption;
import org.apache.apex.malhar.stream.window.WindowState;
import org.apache.apex.malhar.stream.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.stream.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.stream.window.impl.KeyedWindowedOperatorImpl;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Duration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * This is an example of using the WindowedOperator concepts to do streaming word count.
 */
public class Application implements StreamingApplication
{
  public static class WordGenerator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Tuple<KeyValPair<String, Long>>> output = new DefaultOutputPort<>();

    private transient BufferedReader reader;

    @Override
    public void setup(Context.OperatorContext context)
    {
      initReader();
    }

    private void initReader()
    {
      try {
        InputStream resourceStream = this.getClass().getResourceAsStream("/wordcount.txt");
        reader = new BufferedReader(new InputStreamReader(resourceStream));
      } catch (Exception ex) {
        throw Throwables.propagate(ex);
      }
    }

    @Override
    public void teardown()
    {
      IOUtils.closeQuietly(reader);
    }

    @Override
    public void emitTuples()
    {
      try {
        String line = reader.readLine();
        if (line == null) {
          reader.close();
          initReader();
        } else {
          // simulate late data
          long timestamp = System.currentTimeMillis() - (long)(Math.random() * 30000);
          Map<String, Long> countMap = new HashMap<>();
          for (String str : line.split("[\\p{Punct}\\s]+")) {
            countMap.put(StringUtils.lowerCase(str), (countMap.containsKey(str)) ? countMap.get(str) + 1 : 1);
          }
          for (Map.Entry<String, Long> entry : countMap.entrySet()) {
            String word = entry.getKey();
            long count = entry.getValue();
            Tuple.TimestampedTuple<KeyValPair<String, Long>> tuple = new Tuple.TimestampedTuple<>(timestamp, new KeyValPair<>(word, count));
            this.output.emit(tuple);
          }
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }

    @Override
    public void endWindow()
    {
      this.output.emit(new Tuple.WatermarkTuple<KeyValPair<String, Long>>(System.currentTimeMillis() - 15000));
    }
  }

  public static class Sum implements Accumulation<Long, Long, Long>
  {
    @Override
    public Long defaultAccumulatedValue()
    {
      return 0L;
    }

    @Override
    public Long accumulate(Long accumulatedValue, Long input)
    {
      return accumulatedValue + input;
    }

    @Override
    public Long merge(Long accumulatedValue1, Long accumulatedValue2)
    {
      return accumulatedValue1 + accumulatedValue2;
    }

    @Override
    public Long getOutput(Long accumulatedValue)
    {
      return accumulatedValue;
    }

    @Override
    public Long getRetraction(Long accumulatedValue)
    {
      return -accumulatedValue;
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    WordGenerator inputOperator = new WordGenerator();
    KeyedWindowedOperatorImpl<String, Long, Long, Long> windowedOperator = new KeyedWindowedOperatorImpl<>();
    Accumulation<Long, Long, Long> sum = new Sum();

    windowedOperator.setAccumulation(sum);
    windowedOperator.setDataStorage(new InMemoryWindowedKeyedStorage<String, Long>());
    windowedOperator.setRetractionStorage(new InMemoryWindowedKeyedStorage<String, Long>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setWindowOption(new WindowOption.TimeWindows(Duration.standardMinutes(1)));
    windowedOperator.setTriggerOption(TriggerOption.AtWatermark().withEarlyFiringsAtEvery(Duration.millis(1000)).accumulatingAndRetractingFiredPanes());
    //windowedOperator.setAllowedLateness(Duration.millis(14000));

    ConsoleOutputOperator outputOperator = new ConsoleOutputOperator();
    dag.addOperator("inputOperator", inputOperator);
    dag.addOperator("windowedOperator", windowedOperator);
    dag.addOperator("outputOperator", outputOperator);
    dag.addStream("input_windowed", inputOperator.output, windowedOperator.input);
    dag.addStream("windowed_output", windowedOperator.output, outputOperator.input);
  }

  public static void main(String[] args) throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run();
  }
}
