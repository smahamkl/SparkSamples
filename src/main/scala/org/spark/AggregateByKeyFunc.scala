package org.spark

import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.HashSet


object AggregateByKeyFunc {

  def myfunc(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
    iter.map(x => "[partID:" + index + ", val: " + x + "]")
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sample2").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    sqlContext.setConf("spark.sql.caseSensitive", "false")

    val pairRDD = sc.parallelize(Array("key1=value1", "key2=value2", "key1=value1", "key1=value3", "key2=value4", "key3=value5", "key4=value6"))

    val pairRDD1 = pairRDD.map(x => x.split("=")).map(x => (x(0), x(1)))

    pairRDD1.aggregateByKey(scala.collection.mutable.HashSet.empty[String])(combineFunc, mergePartitions).foreach(println)


    def combineFunc(myset: HashSet[String], curEle: String): HashSet[String] = {

      myset += curEle
    }

    def mergePartitions(myset1: HashSet[String], myset2: HashSet[String]): HashSet[String] = {
      myset1 ++ myset2
    }
  }

}