package org.spark

import org.apache.spark.{SparkConf, SparkContext}

object SampleTextProcessing {

  def main(args: Array[String]) = {

    val sc = new SparkContext(new SparkConf().setAppName("Sample Text Processing").setMaster("local"))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    //    val rdd1 =  sc.parallelize(
    //      List(
    //        "yellow",   "red",
    //        "blue",     "cyan",
    //        "black"
    //      ), 3)
    //
    //    val mapped =   rdd1.mapPartitionsWithIndex {
    //      (index, iterator) => {
    //        println("Called in Partition -> " + index)
    //        val myList = iterator.toList
    //
    //        myList.map(x => x + " -> " + index).iterator
    //      }
    //    }
    //    mapped.collect().foreach(println)


    val strPath = "D:/data_repo/sample.txt"

    val input = sc.textFile(strPath, minPartitions = 2)

    val input2 = input.mapPartitionsWithIndex { (idx, iter) =>
      if (idx == 0) iter.drop(1) else iter
    }

    val pairs = input2.flatMap(line => line.split(",").zipWithIndex.map{
      case ("true", i) => (i, "1")
      case ("false", i) => (i, "0")
      //case p => (p._1, p._2)
      case p => p.swap
    })

    val result = pairs.groupByKey.map{
      case (k, vals) =>  {
        val valsString = vals.mkString("|")
        s"$k,$valsString"
      }
    }.foreach(println)


    System.exit(0)

    val delim1 = "\001"

    def separateCols(line: String): Array[String] = {
      val line2 = line.replaceAll("true", "1")
      val line3 = line2.replaceAll("false", "0")
      val vals: Array[String] = line3.split(",")

      for ((x, i) <- vals.view.zipWithIndex) {
        vals(i) = "VAR_%04d".format(i) + delim1 + x
      }
      vals
    }

    val input3 = input2.flatMap(separateCols)

    def toKeyVal(line: String): (String, String) = {
      val vals = line.split(delim1)
      (vals(0), vals(1))
    }

    val input4 = input3.map(toKeyVal)

    def valsConcat(val1: String, val2: String): String = {
      val1 + "," + val2
    }

    val input5 = input4.reduceByKey(valsConcat)

    input5.saveAsTextFile("D:/data_repo/airline_output")
  }

}
