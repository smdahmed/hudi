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

import com.uber.hoodie.DataSourceWriteOptions
import com.uber.hoodie.common.model.HoodieKey
import com.uber.hoodie.common.model.HoodieRecord
import com.uber.hoodie.common.util.HoodieAvroUtils
import com.uber.hoodie.config.HoodieWriteConfig

import java.sql.Timestamp

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object MARApp {

  /*
   * trade_date : String
   * The reason why trade_date is being stored as String is for the reasons below:
   * 1. Parquet does not support Date.
   * 2. Hive has issues reading parquet Timestamp stored in parquet as String.
   * 3. Hive has issues retrieving timestamp from Long type in parquet.
   *
   * For further details please refer: https://gist.github.com/smdahmed/e27559f52764748fce0d33133aada7fa
   */

  case class HudiRecord(_row_key: String, trade_date: String, bats: String)

  def rddRemoveDuplicates(inputDF: DataFrame) : RDD[String] = {

    inputDF.rdd // Retrieve RDD from the dataframe
      .map(_.mkString(",")) // Add a , separator
      .map(row => (row.split(",")(0), row)) // Split the row into rowkey vs row
      .reduceByKey((dup, dup1) => null) // Remove the duplicates by doing reduceByKey to null
      .filter(row => if (row._2 == null) false else true) // filter the rows having empty values to remove duplicates
      .values // we do not need the row key that was created. Pick up the values now

  }

  def convertToHoodieString(row: String) : String = {

    val _row_key = row.split(",")(0).trim
    val trade_date = row.split(",")(1).trim
    val bats = row.split(",")(2).trim.toInt // Retrieve Int from String

    val partitionPath = trade_date.replace("-", "/")
    val key = new HoodieKey(_row_key, partitionPath)
    val hoodieRecord = new HoodieRecord[TestRawTripPayload](key,
      generateHudiRecords(key, _row_key, trade_date, bats))

    val opt = DataSourceTestUtils.convertToString(hoodieRecord)

    if (!opt.isPresent) throw new RuntimeException("bad error")

    opt.get()

  }

  def generateHudiRecords(key: HoodieKey, _row_key: String, trade_date: String, bats: Int) = {

    val KUDI_EXAMPLE_SCHEMA = "{\"type\": \"record\"," + "\"name\": \"hudirec\"," + "\"fields\": [ " +
      "{\"name\": \"timestamp\",  \"type\": \"double\"}," +
      "{\"name\": \"_row_key\",   \"type\": \"string\"}," +
      "{\"name\": \"trade_date\", \"type\": \"string\"}," +
      "{\"name\": \"bats\",       \"type\": \"int\"}]}";

    val avroSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(KUDI_EXAMPLE_SCHEMA))

    val rec = new GenericData.Record(avroSchema)

    rec.put("timestamp", 0.0)
    rec.put("_row_key", _row_key)
    rec.put("trade_date", trade_date)
    rec.put("bats", bats)

    HoodieAvroUtils.addCommitMetadataToRecord(rec, new Timestamp(System.currentTimeMillis()).toString, "-1")

    new TestRawTripPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), KUDI_EXAMPLE_SCHEMA)

  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Kudi")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val schema = Encoders.product[HudiRecord].schema

    /* Read and cache the CSV file as this will be the base of all the DataFrames that will be generated */
    val inputDF = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("hdfs://quickstart.cloudera/user/cloudera/javaapp/input.csv")
      .cache()

    /*
     * Generate a RDD that contains no duplicates
     */
    val rddWithoutDuplicates = rddRemoveDuplicates(inputDF)

    // Convert RDD without duplicates to hoodie RDD
    val hoodieRDD: org.apache.spark.rdd.RDD[String] = rddWithoutDuplicates.map(row => convertToHoodieString(row))

    // convert hoodie RDD to dataframe
    val hoodieInsertDF = spark.read.json(hoodieRDD)

    // Save as hoodie dataset (copy on write)
    var writer = hoodieInsertDF.write.format("com.uber.hoodie") // specify the hoodie source
      .option("hoodie.insert.shuffle.parallelism",
      "2") // any hoodie client config can be passed like this
      .option("hoodie.upsert.shuffle.parallelism",
      "2") // full list in HoodieWriteConfig & its package
      .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY, "COPY_ON_WRITE") // Hoodie Table Type
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, "insert") // insert
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key") // This is the record key
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "partition") // this is the partition
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp") // use to combine duplicate
      .option(HoodieWriteConfig.TABLE_NAME, "kabeer_test") // Used by hive sync and queries
      .mode(SaveMode.Overwrite); // This will remove any existing data at path below, and create a

    writer.save("hdfs://quickstart.cloudera/user/cloudera/kabeer_test/")

    val updateDF = spark.read
      .option("header", "true")
      .schema(schema)
      .csv("hdfs://quickstart.cloudera/user/cloudera/javaapp/update.csv")
      .cache()

    val updateRDDWithoutDuplicates = rddRemoveDuplicates(updateDF)

    val updateHoodieRDD: org.apache.spark.rdd.RDD[String] =
      updateRDDWithoutDuplicates.map(row => convertToHoodieString(row))

    val updateJSONDF = spark.read.json(updateHoodieRDD)

    // Save as hoodie dataset (copy on write)
    writer = updateJSONDF.write.format("com.uber.hoodie") // specify the hoodie source
      .option("hoodie.insert.shuffle.parallelism",
      "2") // any hoodie client config can be passed like this
      .option("hoodie.upsert.shuffle.parallelism",
      "2") // full list in HoodieWriteConfig & its package
      .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY, "COPY_ON_WRITE") // Hoodie Table Type
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY,
      "_row_key") // This is the record key
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY,
      "partition") // this is the partition to place it into
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY,
      "timestamp") // use to combine duplicate records in input/with disk val
      .option(HoodieWriteConfig.TABLE_NAME, "kabeer_test") // Used by hive sync and queries
      .mode(
      SaveMode.Append); // This will remove any existing data at path below, and create a

    writer.save("hdfs://quickstart.cloudera/user/cloudera/kabeer_test/")

  }

}
