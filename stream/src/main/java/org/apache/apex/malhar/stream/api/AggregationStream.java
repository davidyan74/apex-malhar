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
package org.apache.apex.malhar.stream.api;

/**
 * A stream with aggregation transformation
 * All aggregation transformation would support
 * @ Watermark - Ingestion time watermark / natural tuple watermark
 * @ Early Triggers - How frequent to emit real-time partial result
 * @ Late Triggers - When to emit updated result with tuple comes after watermark
 * @ Spool state -  When the in memory aggregation run out of memory
 * @ 3 different aggregation model: ignore, accumulation, accumulation + delta
 * @ Window support, sliding window, moving window, session window base on 3 different tuple time
 * @ 3 Differernt tuple time support: event time, system time, ingestion time
 * @param <T>
 */
public interface AggregationStream<T> extends ApexStream<T>
{
}
