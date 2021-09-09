package com.h3c.spark.hudi.example

import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.QuickstartUtils._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

object SparkHoodieExample {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("SparkHudiExample").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

    val tableName = "hudi_cow_table"
    val basePath = "/tmp/hudi_cow_table"
    val dataGen = new DataGenerator()

    // Insert data
    val inserts = convertToStringList(dataGen.generateInserts(10))
    val df = spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json(spark.sparkContext.parallelize(inserts, 2))
    df.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Overwrite).
      save(basePath)

    // Query data
    val tripsSnapshotDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")
    //load(basePath) use "/partitionKey=partitionValue" folder structure for Spark auto partition discovery
    tripsSnapshotDF.createOrReplaceTempView("hudi_trips_snapshot")

    spark.sql("select fare, begin_lon, begin_lat, ts from  hudi_trips_snapshot where fare > 20.0").show()
    spark.sql("select _hoodie_commit_time, _hoodie_record_key, _hoodie_partition_path, rider, driver, fare from  hudi_trips_snapshot").show()

    // update data
    val updates = convertToStringList(dataGen.generateUpdates(10))
    val updateDf = spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json(spark.sparkContext.parallelize(updates, 2))
    updateDf.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    // reload data
    spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*").
      createOrReplaceTempView("hudi_trips_snapshot")

    implicit val encoder=org.apache.spark.sql.Encoders.STRING//添加字符串类型编码器
    val commits = spark.sql("select distinct(_hoodie_commit_time) as commitTime from  hudi_trips_snapshot order by commitTime").
      map(k => k.getString(0)).take(50)
    val beginTime = commits(commits.length - 2) // commit time we are interested in

    // incrementally query data
    val tripsIncrementalDF = spark.read.format("hudi").
      option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY, beginTime).
      load(basePath)
    tripsIncrementalDF.createOrReplaceTempView("hudi_trips_incremental")

    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from  hudi_trips_incremental where fare > 20.0").show()

    // Point in time query
    val pointBeginTime = "000" // Represents all commits > this time.
    val pointEndTime = commits(commits.length - 2) // commit time we are interested in

    //incrementally query data
    val tripsPointInTimeDF = spark.read.format("hudi").
      option(QUERY_TYPE_OPT_KEY, QUERY_TYPE_INCREMENTAL_OPT_VAL).
      option(BEGIN_INSTANTTIME_OPT_KEY, pointBeginTime).
      option(END_INSTANTTIME_OPT_KEY, pointEndTime).
      load(basePath)
    tripsPointInTimeDF.createOrReplaceTempView("hudi_trips_point_in_time")
    spark.sql("select `_hoodie_commit_time`, fare, begin_lon, begin_lat, ts from hudi_trips_point_in_time where fare > 20.0").show()

    // delete data
    // fetch total records count
    spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
    // fetch two records to be deleted
    val ds = spark.sql("select uuid, partitionpath from hudi_trips_snapshot").limit(2)

    // issue deletes
    val deletes = dataGen.generateDeletes(ds.collectAsList())
    val deleteDf = spark.read.option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ").json(spark.sparkContext.parallelize(deletes, 2))

    deleteDf.write.format("hudi").
      options(getQuickstartWriteConfigs).
      option(OPERATION_OPT_KEY,"delete").
      option(PRECOMBINE_FIELD_OPT_KEY, "ts").
      option(RECORDKEY_FIELD_OPT_KEY, "uuid").
      option(PARTITIONPATH_FIELD_OPT_KEY, "partitionpath").
      option(TABLE_NAME, tableName).
      mode(Append).
      save(basePath)

    // run the same read query as above.
    val roAfterDeleteViewDF = spark.
      read.
      format("hudi").
      load(basePath + "/*/*/*/*")

    roAfterDeleteViewDF.registerTempTable("hudi_trips_snapshot")
    // fetch should return (total - 2) records
    spark.sql("select uuid, partitionpath from hudi_trips_snapshot").count()
  }

}
