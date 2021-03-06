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

package com.macquarie.mar

import org.apache.spark.sql.SparkSession

object KudiScalaApp1 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Scala Example").getOrCreate()

    val rdd = spark.read.csv("hdfs://quickstart.cloudera/user/cloudera/javaapp/input.csv")
      .rdd
      .map(_.mkString(","))
      .map(line => line.split(","))
      .flatMap(x => x)
      .map(x => (x, 1))
      .toJavaRDD()
      .saveAsTextFile("hdfs://quickstart.cloudera/user/cloudera/javaapp/wordcount.txt")


  }

}
