package org.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkSample3 {

  def main(args: Array[String]):Unit = {

    val conf = new SparkConf().setAppName("SparkSample3").setMaster("local")

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)

    val fileRDD = sc.textFile("/home/sivam/Documents/data_repo/pagecounts.gz")

    val parsedRDD = fileRDD.flatMap { row =>
      row.split("""\s++""") match {

        case Array(project, page, numrequests, _) => Some((project, page, numrequests))
        case _ => None
      }
    }

    parsedRDD.filter(x => x._1 == "en").map(x => (x._2, x._3.toInt)).reduceByKey((x,y) => x + y).sortBy(x => x._2, false).take(100).foreach(println)
  }

/*  def maxi(m: String, n: String): String = {
    if (m > n)
      m
    else
      n

  }*/

}
