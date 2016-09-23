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
    Combine<Long> cb = new Combine<Long>();
    List<Set<Long>> accu = cb.defaultAccumulatedValue();

    Assert.assertEquals(2, accu.size());
    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(0, accu.get(i).size());
    }

    for (long i = 1; i <= 3; i++) {
      accu = cb.accumulate(accu, i);
      accu = cb.accumulate2(accu, i * 2);
    }

    for (int i = 0; i < 2; i++) {
      Assert.assertEquals(3, accu.get(i).size());
    }

    Assert.assertEquals(9, cb.getOutput(accu).size());
    for (int i = 0; i < 9; i++) {
      Assert.assertEquals(2, cb.getOutput(accu).get(i).size());
    }
  }
}
