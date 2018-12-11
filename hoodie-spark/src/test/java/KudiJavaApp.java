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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import com.uber.hoodie.DataSourceWriteOptions;
import com.uber.hoodie.HoodieDataSourceHelpers;

import com.uber.hoodie.common.TestRawTripPayload;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTableType;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.hive.MultiPartKeysValueExtractor;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Sample program that writes & reads hoodie datasets via the Spark datasource
 */
public class KudiJavaApp {
  @Parameter(names = {"--table-path", "-p"}, description = "path for Hoodie sample table")
    private String tablePath = "file:///tmp/hoodie/sample-table";

  @Parameter(names = {"--table-name", "-n"}, description = "table name for Hoodie sample table")
    private String tableName = "hoodie_test";

  @Parameter(names = {"--table-type", "-t"}, description = "One of COPY_ON_WRITE or MERGE_ON_READ")
    private String tableType = HoodieTableType.COPY_ON_WRITE.name();

  @Parameter(names = {"--hive-sync", "-hv"}, description = "Enable syncing to hive")
    private Boolean enableHiveSync = false;

  @Parameter(names = {"--hive-db", "-hd"}, description = "hive database")
    private String hiveDB = "default";

  @Parameter(names = {"--hive-table", "-ht"}, description = "hive table")
    private String hiveTable = "hoodie_sample_test";

  @Parameter(names = {"--hive-user", "-hu"}, description = "hive username")
    private String hiveUser = "hive";

  @Parameter(names = {"--hive-password", "-hp"}, description = "hive password")
    private String hivePass = "hive";

  @Parameter(names = {"--hive-url", "-hl"}, description = "hive JDBC URL")
    private String hiveJdbcUrl = "jdbc:hive2://localhost:10000";

  @Parameter(names = {"--use-multi-partition-keys", "-mp"}, description = "Use Multiple Partition Keys")
    private Boolean useMultiPartitionKeys = false;

  @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

  private static Logger logger = LogManager.getLogger(HoodieJavaApp.class);

  public static void main(String[] args) throws Exception {
    KudiJavaApp cli = new KudiJavaApp();
    JCommander cmd = new JCommander(cli, args);

    if (cli.help) {
      cmd.usage();
      System.exit(1);
    }

    cli.run();
  }

  public void run() throws Exception {

    // Spark session setup..
    SparkSession spark = SparkSession.builder().appName("Hoodie Spark APP")
            .config("spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer").master("local[1]")
            .getOrCreate();
    JavaSparkContext jssc = new JavaSparkContext(spark.sparkContext());
    FileSystem fs = FileSystem.get(jssc.hadoopConfiguration());

    // Generator of some records to be loaded in.
    // HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    /**
     * Commit with only inserts
     */

    // Generate some input..
    // List<String> records1 = DataSourceTestUtils.convertToStringList(
    //        dataGen.generateInserts("001"/* ignore */, 100));
    // Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 2));

    /**
     * convert each row in a csv to HoodieRecord
     */

    /* Insert the records */
    List<String> records1 = DataSourceTestUtils.convertToStringList(generateRecords(
            "/kabeer/code/myhudi/input.csv",
            new Timestamp(System.currentTimeMillis()).toString()
    ));

    Dataset<Row> inputDF1 = spark.read().json(jssc.parallelize(records1, 2));

    // Save as hoodie dataset (copy on write)
    DataFrameWriter<Row> writer = inputDF1.write().format("com.uber.hoodie") // specify the hoodie source
            .option("hoodie.insert.shuffle.parallelism",
                    "2") // any hoodie client config can be passed like this
            .option("hoodie.upsert.shuffle.parallelism",
                    "2") // full list in HoodieWriteConfig & its package
            .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY(), tableType) // Hoodie Table Type
            .option(DataSourceWriteOptions.OPERATION_OPT_KEY(),
                    DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL()) // insert
            .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(),
                    "_row_key") // This is the record key
            .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(),
                    "partition") // this is the partition to place it into
            .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(),
                    "timestamp") // use to combine duplicate records in input/with disk val
            .option(HoodieWriteConfig.TABLE_NAME, tableName) // Used by hive sync and queries
            .mode(
                    SaveMode.Overwrite); // This will remove any existing data at path below, and create a

    updateHiveSyncConfig(writer);
    // new dataset if needed
    writer.save(tablePath); // ultimately where the dataset will be placed
    String commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    logger.info("First commit at instant time :" + commitInstantTime1);

    /* Update the records */
    List<String> records2 = DataSourceTestUtils.convertToStringList(generateRecords(
            "/kabeer/code/myhudi/update.csv",
            new Timestamp(System.currentTimeMillis()).toString()
    ));

    Dataset<Row> inputDF2 = spark.read().json(jssc.parallelize(records2, 2));

    // Save as hoodie dataset (copy on write)
    writer = inputDF2.write().format("com.uber.hoodie") // specify the hoodie source
            .option("hoodie.insert.shuffle.parallelism",
                    "2") // any hoodie client config can be passed like this
            .option("hoodie.upsert.shuffle.parallelism",
                    "2") // full list in HoodieWriteConfig & its package
            .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY(), tableType) // Hoodie Table Type
            .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(),
                    "_row_key") // This is the record key
            .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(),
                    "partition") // this is the partition to place it into
            .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY(),
                    "timestamp") // use to combine duplicate records in input/with disk val
            .option(HoodieWriteConfig.TABLE_NAME, tableName) // Used by hive sync and queries
            .mode(
                    SaveMode.Append); // This will remove any existing data at path below, and create a

    updateHiveSyncConfig(writer);
    // new dataset if needed
    writer.save(tablePath); // ultimately where the dataset will be placed
    String commitInstantTime2 = HoodieDataSourceHelpers.latestCommit(fs, tablePath);
    logger.info("First commit at instant time :" + commitInstantTime2);

  }

  /**
   * Setup configs for syncing to hive
   *
   * @param writer
   * @return
   */
  private DataFrameWriter<Row> updateHiveSyncConfig(DataFrameWriter<Row> writer) {
    if (enableHiveSync) {
      logger.info("Enabling Hive sync to " + hiveJdbcUrl);
      writer = writer.option(DataSourceWriteOptions.HIVE_TABLE_OPT_KEY(), hiveTable)
              .option(DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY(), hiveDB)
              .option(DataSourceWriteOptions.HIVE_URL_OPT_KEY(), hiveJdbcUrl)
              .option(DataSourceWriteOptions.HIVE_USER_OPT_KEY(), hiveUser)
              .option(DataSourceWriteOptions.HIVE_PASS_OPT_KEY(), hivePass)
              .option(DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY(), "true");
      if (useMultiPartitionKeys) {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "year,month,day")
                  .option(DataSourceWriteOptions.HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY(),
                          MultiPartKeysValueExtractor.class.getCanonicalName());
      } else {
        writer = writer.option(DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY(), "dateStr");
      }
    }
    return writer;
  }

  /* Kudi Methods */
  public List<HoodieRecord> generateRecords(String filePath, String commitTime) throws IOException {

    List<HoodieRecord> records = new ArrayList<>();

    Reader in = new FileReader(filePath);
    Iterable<CSVRecord> inputRecords = CSVFormat.RFC4180.withFirstRecordAsHeader().parse(in);

    for (CSVRecord record : inputRecords) {

      String partitionPath = record.get("trade_date").replace("-", "/");
      HoodieKey key = new HoodieKey(record.get("_row_key"), partitionPath);
      HoodieRecord hoodieRecord = new HoodieRecord(key, generateKudiRecords(key, record, commitTime));

      records.add(hoodieRecord);

    }

    return records;
  }

  public static String KUDI_EXAMPLE_SCHEMA = "{\"type\": \"record\"," + "\"name\": \"kudirec\"," + "\"fields\": [ "
          + "{\"name\": \"timestamp\",  \"type\": \"double\"},"
          + "{\"name\": \"_row_key\",   \"type\": \"string\"},"
          + "{\"name\": \"trade_date\", \"type\": \"string\"},"
          + "{\"name\": \"bats\",       \"type\": \"string\"}]}";

  public static Schema avroSchema = HoodieAvroUtils.addMetadataFields(new Schema.Parser().parse(KUDI_EXAMPLE_SCHEMA));

  public static TestRawTripPayload generateKudiRecords(HoodieKey key, CSVRecord record, String commitTime)
          throws IOException {

    GenericRecord rec = new GenericData.Record(avroSchema);

    rec.put("timestamp", 0.0);
    rec.put("_row_key", key.getRecordKey());
    rec.put("trade_date", record.get("trade_date"));
    rec.put("bats", record.get("bats"));

    HoodieAvroUtils.addCommitMetadataToRecord(rec, commitTime, "-1");
    return new TestRawTripPayload(rec.toString(), key.getRecordKey(), key.getPartitionPath(), KUDI_EXAMPLE_SCHEMA);

  }

}
