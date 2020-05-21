package org.spark

import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.hdfs.protocol.proto.ClientNamenodeProtocolProtos.SaveNamespaceRequestProto
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameReader, Row, SQLContext, Dataset}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.hadoop.io.NullWritable

case class Person(name: String, age: Long)

object Sample1 {


  def main(args: Array[String]): Unit = {



    val conf = new SparkConf().setAppName("GMRFrameworkFinalJoin").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.caseSensitive", "false")


    //val ds = Seq(1, 2, 3).toDS()
    //ds.map(_ + 1).collect() // Returns: Array(2, 3, 4)

    import sqlContext.implicits._

    //val ds = Seq(Person("Andy", 32)).toDS()
    // DataFrames can be converted to a Dataset by providing a class. Mapping will be done by name.
    val path = "src/main/resources/people.json"

    import org.apache.spark.sql.DataFrame

    val df  = sqlContext.read.json(path).as[Person]
    df.show()



    val xs = Seq(("a", "foo", 2.0), ("x", "bar", -1.0)).toDS
    val ys = Seq(("a", "foo", 2.0), ("y", "bar", 1.0)).toDS

    xs.as("xs").joinWith(ys.as("ys"), ($"xs._1" === $"ys._1") && ($"xs._2" === $"ys._2"), "left").show()

//
////    val segments = sqlContext.read.format("com.databricks.spark.csv")
////      .option("delimiter", "\t")
////      .load("/Users/smahamka/Desktop/Latest_1004.txt")
//
//    val cefFile = sc.textFile("/Users/smahamka/Desktop/Latest_1004.txt")
//    val joinedRDD = cefFile.map(x => x.split("\\t")).map(x =>(x(1),(try{ x(5).toFloat }, 1)))
//
//    //joinedRDD.take(5).foreach { println }
//
//    val y = joinedRDD.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(y=>y._1/y._2).collect()
//
//    //val joinedRDD1 = joinedRDD.map((x,y)=>
//    //val cefDim1 =  cefFile.filter { line => line.startsWith("1") }
//
//    y.take(50).foreach { println }
//
//    //val joinedRDD1 = joinedRDD.map(x => (x._1, x._2)).collect
//    //
//    //joinedRDD1.take(5).foreach { println }
//
//    println()

  }

}
