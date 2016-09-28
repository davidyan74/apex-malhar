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

import org.junit.Assert;
import org.junit.Test;
import org.apache.apex.malhar.lib.window.WindowState;
import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 *  Test of windowed join operator handling watermarks.
 */
public class WindowedJoinOperatorWatermarkTest
{
  @Test
  public void WindowedJoinOperatorWatermarkTest()
  {
    WindowedJoinOperatorImpl op = new WindowedJoinOperatorImpl();
    op.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    CollectorTestSink<WatermarkImpl> sink = new CollectorTestSink<>();
    op.controlOutput.setSink(sink);

    // No watermark is generated if the join operator haven't seen all watermarks from all input streams.
    op.controlInput.process(new WatermarkImpl(1000000));
    op.endWindow();
    Assert.assertEquals(-1, op.currentWatermark);
    Assert.assertEquals(0, sink.collectedTuples.size());

    // Once both input streams sent watermarks to join operator, it should generate a watermark and send to downstream.
    op.controlInput2.process(new WatermarkImpl(200000));
    op.endWindow();
    Assert.assertEquals(200000, op.currentWatermark);
    Assert.assertEquals(1, sink.collectedTuples.size());

    // If the minimum of the latest input watermarks changes, join operator should also generate a new watermark.
    op.controlInput2.process(new WatermarkImpl(2100000));
    op.endWindow();
    Assert.assertEquals(1000000, op.currentWatermark);
    Assert.assertEquals(2, sink.collectedTuples.size());

    // Current watermark of join operator could only change during endWindow() event.
    op.controlInput.process(new WatermarkImpl(1100000));
    Assert.assertEquals(1000000, op.currentWatermark);
    op.endWindow();
    Assert.assertEquals(1100000, op.currentWatermark);
    Assert.assertEquals(3, sink.collectedTuples.size());

    // If the upstreams sent a watermark but the minimum of the latest input watermarks doesn't change, the join
    // operator should not generate a new watermark, thus nothing will be sent to downstream.
    op.controlInput.process(new WatermarkImpl(1100000));
    op.endWindow();
    Assert.assertEquals(1100000, op.currentWatermark);
    Assert.assertEquals(3, sink.collectedTuples.size());
  }

}
