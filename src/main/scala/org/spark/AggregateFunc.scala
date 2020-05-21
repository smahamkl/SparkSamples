package org.spark

import org.apache.spark.{SparkConf, SparkContext}

object AggregateFunc {

  // lets first print out the contents of the RDD with partition labels
  def myfunc(index: Int, iter: Iterator[(Int)]) : Iterator[String] = {
    iter.map(x => "[partID:" +  index + ", val: " + x + "]")
  }

  def myfunc1(index: Int, iter: Iterator[(String)]) : Iterator[String] = {
    iter.map(x => "[partID:" +  index + ", val: " + x + "]")
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sample2").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.caseSensitive", "false")

    val z = sc.parallelize(List(1,2,3,4,5,6),2)

    z.mapPartitionsWithIndex(myfunc).collect.foreach(println)

    println(z.aggregate(0)(_ + _, _ + _))
    // This example returns 16 since the initial value is 5
    // reduce of partition 0 will be max(5, 1, 2, 3) = 5
    // reduce of partition 1 will be max(5, 4, 5, 6) = 6
    // final reduce across partitions will be 5 + 5 + 6 = 16
    // note the final reduce include the initial value
    println(z.aggregate(5)(math.max(_, _), _ + _))


    val z1 = sc.parallelize(List("a","b","c","d","e","f"),2)

    z1.mapPartitionsWithIndex(myfunc1).collect

    println(z1.aggregate("")(_ + _, _+_))

    // See here how the initial value "x" is applied three times.
    //  - once for each partition
    //  - once when combining all the partitions in the second reduce function.
    println(z1.aggregate("x")(_ + _, _+_))

    // Below are some more advanced examples. Some are quite tricky to work out.

    val z2 = sc.parallelize(List("12","23","345","4567"),2)

    z2.mapPartitionsWithIndex(myfunc1).collect.foreach(println)

    println(z2.aggregate("")((x,y) => math.max(x.length, y.length).toString, (x,y) => x + y))

    println(z2.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y))

    val z3 = sc.parallelize(List("12","23","345",""),2)
    println(z3.aggregate("")((x,y) => math.min(x.length, y.length).toString, (x,y) => x + y))

  }

}
