package org.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object AggregateDIstinct {

  def main(args: Array[String]) = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sc = new SparkContext(new SparkConf().setAppName("Grouping Examples").setMaster("local"))

    val keysWithValuesList = Array("foo=A", "foo=A", "foo=A", "foo=A", "foo=B", "bar=C", "bar=D", "bar=D")

    val data = sc.parallelize(keysWithValuesList)

    //Create key value pairs
    val kv = data.map(_.split("=")).map(v => (v(0), v(1))).cache()

    val initialSet = mutable.HashSet.empty[String]
    val addToSet = (s: mutable.HashSet[String], v: String) => s += v
    val mergePartitionSets = (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2

    val uniqueByKey = kv.aggregateByKey(initialSet)(addToSet, mergePartitionSets)

    val initialCount = 0
    val addToCounts = (n: Int, v: String) => n + 1
    val sumPartitionCounts = (p1: Int, p2: Int) => p1 + p2

    val countByKey = kv.aggregateByKey(initialCount)(addToCounts, sumPartitionCounts)


    println("Aggregate By Key unique Results")

    val uniqueResults = uniqueByKey.collect()

   for(indx <- uniqueResults.indices){
      val r = uniqueResults(indx)
      println(r._1 + " -> " + r._2.mkString(","))
    }

    System.exit(0)

    println("------------------")

    println("Aggregate By Key sum Results")
    val sumResults = countByKey.collect()
    for(indx <- sumResults.indices){
      val r = sumResults(indx)
      println(r._1 + " -> " + r._2)
    }

    var a= List("a", "b", "c", "d", "e")
    println(list2String(a))

    var a1= Seq("a1", "b1", "c1", "d1", "e1")
    println(list2String(a1))

  }

  def list2String(list: Seq[String]) :String ={

    list match {
      case a::rest => a + " " + list2String(rest)
      case Nil=> ""
    }
  }

}
