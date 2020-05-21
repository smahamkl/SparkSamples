package org.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSample2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sample Spark App2").setMaster("local")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    conf.set("spark.speculation", "false")
    conf.set("spark.sql.parquet.enableVectorizedReader", "false")

    var fileRDD = sc.textFile("/home/sivam/Documents/data_repo/airlinedata.csv")

    println(fileRDD.count())

    var fileRDD1 = fileRDD.flatMap(x => x.split(",")).map(x => (x, 1)).reduceByKey(_ + _)

    //fileRDD1.sortBy(x => x._2, false).take(20).foreach(println)
    fileRDD1.filter({ x =>
       (x._2 >= 5)
    }).foreach(println)

  }

}
