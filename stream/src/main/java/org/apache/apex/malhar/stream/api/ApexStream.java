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


import java.util.Map;

import org.apache.apex.malhar.stream.api.function.Function;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * The stream interface to build a DAG
 * @param <T>
 */
public interface ApexStream<T>
{
  /**
   * Simple map transformation
   * Add an operator to the DAG which convert tuple T to tuple O
   * @param mapFunction map function
   * @param <O> Type of the output
   * @return
   */
  <O> ApexStream<O> map(Function.MapFunction<T, O> mapFunction);

  /**
   * Simple map transformation
   * Add an operator to the DAG which convert tuple T to tuple O
   * @param name operator name
   * @param mapFunction map function
   * @param <O> Type of the output
   * @return
   */
  <O> ApexStream<O> map(String name, Function.MapFunction<T, O> mapFunction);

  /**
   * Flat map transformation
   * Add an operator to the DAG which convert tuple T to a collection of tuple O
   * @param flatten flat map
   * @param <O> Type of the output
   * @return
   */
  <O> ApexStream<O> flatMap(Function.FlatMapFunction<T, O> flatten);

  /**
   * Flat map transformation
   * Add an operator to the DAG which convert tuple T to a collection of tuple O
   * @param name operator name
   * @param flatten
   * @param <O> Type of the output
   * @return
   */
  <O> ApexStream<O> flatMap(String name, Function.FlatMapFunction<T, O> flatten);

  /**
   * Filter transformation
   * Add an operator to the DAG which filter out tuple T that cannot satisfy the FilterFunction
   * @param filter filter function
   * @return
   */
  ApexStream<T> filter(Function.FilterFunction<T> filter);

  /**
   * Filter transformation
   * Add an operator to the DAG which filter out tuple T that cannot satisfy the FilterFunction
   * @param name operator name
   * @param filter filter function
   * @return
   */
  ApexStream<T> filter(String name, Function.FilterFunction<T> filter);

  /**
   * Reduce transformation
   * Add an operator to the DAG which merge tuple t1, t2 to new tuple
   * @param reduce reduce function
   * @return
   */
  ApexStream<T> reduce(Function.ReduceFunction reduce);

  /**
   * Reduce transformation
   * Add an operator to the DAG which merge tuple t1, t2 to new tuple
   * @param name operator name
   * @param reduce reduce function
   * @return
   */
  ApexStream<T> reduce(String name, Function.ReduceFunction reduce);

  /**
   * Fold transformation
   * Add an operator to the DAG which merge tuple T to accumulated result tuple O
   * @param initialValue initial result value
   * @param fold fold function
   * @param <O> Result type
   * @return
   */
  <O> ApexStream<O> fold(O initialValue, Function.FoldFunction fold);

  /**
   * Fold transformation
   * Add an operator to the DAG which merge tuple T to accumulated result tuple O
   * @param name name of the operator
   * @param initialValue initial result value
   * @param fold fold function
   * @param <O> Result type
   * @return
   */
  <O> ApexStream<O> fold(String name, O initialValue, Function.FoldFunction fold);

  /**
   * Count of all tuples
   * @return
   */
  ApexStream<Integer> count();

  /**
   * Count tuples by the key
   * If the input is KeyedTuple it will get the key from getKey method from the tuple
   * If not, use the tuple itself as a key
   * @param <O>
   * @return
   */
  <O> ApexStream<Map<O, Integer>> countByKey();

  /**
   *
   * Count tuples by the indexed key
   * @param key
   * @param <O>
   * @return
   */
  <O> ApexStream<Map<O, Integer>> countByKey(int key);

  /**
   * Extend the dag by adding one operator
   * @param op
   * @param inputPort
   * @param outputPort
   * @param <O>
   * @return
   */
  <O> ApexStream<O> addOperator(Operator op, Operator.InputPort<T> inputPort,  Operator.OutputPort<O> outputPort);

  /**
   * Extend the dag by adding one operator
   * @param opName
   * @param op
   * @param inputPort
   * @param outputPort
   * @param <O>
   * @return
   */
  <O> ApexStream<O> addOperator(String opName, Operator op, Operator.InputPort<T> inputPort,  Operator.OutputPort<O> outputPort);

  /**
   * Union multiple stream into one
   * @param others
   * @return
   */
  ApexStream<T> union(ApexStream<T>... others);

  /**
   * Add a stdout console output operator
   * @return
   */
  ApexStream<T> print();

  /**
   * Add a stderr console output operator
   * @return
   */
  ApexStream<T> printErr();

  /**
   * Set the attribute value
   * @param attribute
   * @param value
   * @return
   */
  ApexStream<T> with(Attribute attribute, Object value);

  /**
   * Set the locality
   * @param locality
   * @return
   */
  ApexStream<T> with(DAG.Locality locality);

  /**
   * Set the property value of the operator
   * @param propName
   * @param value
   * @return
   */
  ApexStream<T> with(String propName, Object value);


  /**
   * Create dag from stream
   * @return
   */
  DAG createDag();

  /**
   * Populate existing dag
   * @return
   */
  void populateDag(DAG dag);


  /**
   * Run the stream application in local mode
   * @param async
   */
  void runLocally(boolean async);

  /**
   * Submit the application to console
   */
  void run();

  /**
   * Run the stream application in local mode in duration milliseconds
   * @param duration
   */
  void runLocally(long duration);
}
