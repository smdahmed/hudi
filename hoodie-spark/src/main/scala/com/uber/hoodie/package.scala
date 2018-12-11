/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package com.uber.hoodie

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}

package object hoodie {

  /**
    * Adds a method, `hoodie`, to DataFrameWriter
    */
  implicit class AvroDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def avro: String => Unit = writer.format("com.uber.hoodie").save
  }

  /**
    * Adds a method, `hoodie`, to DataFrameReader
    */
  implicit class AvroDataFrameReader(reader: DataFrameReader) {
    def avro: String => DataFrame = reader.format("com.uber.hoodie").load
  }

}
