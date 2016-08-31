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

/**
 *
 */
public class WindowedJoinOperatorWatermarkTest
{
  @Test
  public void WindowedJoinOperatorWatermarkTest()
  {
    WindowedJoinOperatorImpl op = new WindowedJoinOperatorImpl();
    op.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    op.controlInput.process(new WatermarkImpl(1000000));
    op.endWindow();
    Assert.assertEquals(1000000, op.currentWatermark);
    op.controlInput2.process(new WatermarkImpl(200000));
    op.endWindow();
    Assert.assertEquals(200000, op.currentWatermark);
    op.controlInput2.process(new WatermarkImpl(20000));
    op.endWindow();
    Assert.assertEquals(20000, op.currentWatermark);
    op.controlInput.process(new WatermarkImpl(100));
    Assert.assertEquals(20000, op.currentWatermark);
    op.endWindow();
    Assert.assertEquals(100, op.currentWatermark);
  }

}
