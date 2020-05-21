package org.spark

import org.apache.spark.{SparkConf, SparkContext}

object Cartesian {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sample2").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.caseSensitive", "false")

    val x = sc.parallelize(List(1,2,3,4,5))
    val y = sc.parallelize(List(6,7,8,9,10))
    x.cartesian(y).collect.foreach(println)

  }

}
