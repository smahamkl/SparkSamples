package org.spark.udaf

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object CustomMeanTest {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)


    val conf = new SparkConf().setAppName("UDAF Sample").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val customMean = new CustomMean()

    // create test dataset
    val data = (1 to 1000).map { x: Int =>
      x match {
        case t if t <= 500 => Row("A", t.toDouble)
        case t => Row("B", t.toDouble)
      }
    }

    // create schema of the test dataset
    val schema = StructType(Array(
      StructField("key", StringType),
      StructField("value", DoubleType)
    ))

    // construct data frame
    val rdd = sc.parallelize(data)
    val df = sqlContext.createDataFrame(rdd, schema)

    // Calculate average value for each group
    df.groupBy("key").agg(
      customMean(df.col("value")).as("custom_mean"),
      avg("value").as("avg")
    ).show()
  }

}
