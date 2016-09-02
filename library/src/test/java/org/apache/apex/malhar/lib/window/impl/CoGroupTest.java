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
public class CoGroupTest
{

  @Test
  public void CoGroupTest()
  {
    int numStreams = 3;
    CoGroup<Long> cg = new CoGroup<Long>(numStreams);
    List<Set<Long>> accu = cg.defaultAccumulatedValue();

    Assert.assertEquals(numStreams, accu.size());
    for (int i = 0; i < numStreams; i++) {
      Assert.assertEquals(0, accu.get(i).size());
    }

    for (long i = 1; i <= numStreams; i++) {
      accu = cg.accumulate(accu, i);
      accu = cg.accumulate2(accu, i * 2);
      accu = cg.accumulate3(accu, i * 3);
      //accu = cg.accumulate4(accu, i * 4);
      //accu = cg.accumulate5(accu, i * 5);
    }

    for (int i = 0; i < numStreams; i++) {
      Assert.assertEquals(3, accu.get(i).size());
    }

    Assert.assertEquals(3, cg.getOutput(accu).size());
    for (int i = 0; i < 3; i++) {
      Assert.assertEquals(3, cg.getOutput(accu).get(i).size());
    }
    Assert.assertTrue(1 == cg.getOutput(accu).get(0).get(0));
    Assert.assertTrue(4 == cg.getOutput(accu).get(1).get(1));
    Assert.assertTrue(9 == cg.getOutput(accu).get(2).get(2));
  }
}
