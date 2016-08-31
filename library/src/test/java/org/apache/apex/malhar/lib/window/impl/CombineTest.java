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

import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link Combine}.
 */
public class CombineTest
{
  @Test
  public void CombineTest()
  {
    int numStreams = 3;
    Combine<Long> cb = new Combine<Long>(numStreams);
    List<Set<Long>> accu = cb.defaultAccumulatedValue();

    Assert.assertEquals(numStreams, accu.size());
    for (int i = 0; i < numStreams; i++) {
      Assert.assertEquals(0, accu.get(i).size());
    }

    for (long i = 1; i <= numStreams; i++) {
      accu = cb.accumulate(accu, i);
      accu = cb.accumulate2(accu, i * 2);
      accu = cb.accumulate3(accu, i * 3);
      //accu = cb.accumulate4(accu, i * 4);
      //accu = cb.accumulate5(accu, i * 5);
    }

    for (int i = 0; i < numStreams; i++) {
      Assert.assertEquals(3, accu.get(i).size());
    }

    Assert.assertEquals(27, cb.getOutput(accu).size());
    for (int i = 0; i < 27; i++) {
      Assert.assertEquals(3, cb.getOutput(accu).get(i).size());
    }
  }
}
