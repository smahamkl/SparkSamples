package org.spark

import org.apache.spark.{SparkConf, SparkContext}

object CoGroupFunc {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Sample2").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.setConf("spark.sql.caseSensitive", "false")

    val a = sc.parallelize(List(1, 2, 1, 3), 1)
    val b = a.map((_, "b"))
    val c = a.map((_, "c"))
    b.cogroup(c).collect.foreach(println)

    val d = a.map((_, "d"))
    b.cogroup(c, d).collect.foreach(println)

    val x = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
    val y = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)
    val grouped = x.cogroup(y).collect.foreach(println)

    sys.exit()

/*    val updated = grouped.map { x => {
      val key = x._1
      //println("Key -> " + key)
      val value = x._2
      val itl1 = value._1
      val itl2 = value._2
      val res1 = itl1.map { x => {
        //println("It1 : Key -> " + key + ", Val -> " + (x + 1))
        x + 1
      }
      }
      val res2 = itl2.map { x => {
        //println("It2 : Key -> " + key + ", Val -> " + (x + 1))
        x + 1
      }
      }
      //println("End")
      println(key, (res1, res2))
    }
    }*/


  }

}
