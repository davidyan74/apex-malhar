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
package org.apache.apex.malhar.sql.sample;

import org.apache.apex.malhar.sql.SQLExecEnvironment;
import org.apache.apex.malhar.sql.table.CSVMessageFormat;
import org.apache.apex.malhar.sql.table.FileEndpoint;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "SQLApplicationWithAPI")
/**
 * @since 3.6.0
 */
public class SQLApplicationWithAPI implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Source definition
    String schemaInName = conf.get("csvSchemaInName");
    String schemaIn = conf.get("csvSchemaIn");
    String sourceFile = conf.get("sourceFile");

    SQLExecEnvironment.getEnvironment()
        .registerTable(schemaInName, new FileEndpoint(sourceFile, new CSVMessageFormat(schemaIn)))
        .executeSQL(dag, conf.get("sql"));
  }
}
