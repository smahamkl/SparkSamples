package org.spark

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SampleAggregateBy {
  def main(args: Array[String]) = {

    val conf = new SparkConf().setAppName("Sample Aggregate By").setMaster("local")

    val sc = new SparkContext(conf)

    val sqlConext = new SQLContext(sc)

    val fileRDD = sc.textFile("/home/sivam/filerepo/babynames.csv")

    val fileRDD1 = fileRDD.map(line => line.split(",")).map(row => (row(1), 2018 - row(0).toInt, 1))

    val fileRDD2 = fileRDD1.filter(row => row._1.toLowerCase == "david" || row._1.toLowerCase == "abbey").map(row=> ((row._1),(row._2, row._3)))

    fileRDD2.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x=> (x._1, (x._2._1.toFloat/x._2._2.toFloat))).foreach(println)

    fileRDD2.aggregateByKey((0,0))(
      (acct,value) => (acct._1 + value._1, acct._2 + value._2),
      (acct1, acct2) => (acct1._1 + acct2._1, acct2._1 + acct2._2)
    ).mapValues(x => (x._1.toFloat / x._2.toFloat)).foreach(println)

    //fileRDD1.sortBy(row => row(2).toFloat, false).take(10).foreach(x=> {println(x(0) + " " + x(1) + " " + x(2) + " " + x(3))})

  }

}
