package org.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SampleZip {

  def main(args: Array[String]) = {

    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sc = new SparkContext(new SparkConf().setAppName("Spark zip").setMaster("local"))

    val personArr = Person1(1, "Siva") :: Person1(2, "Sai") :: Person1(3, "Gayathri") :: Nil
    val personPurchaseArr = Purchase(1, 1.2, "IT") :: Purchase(2, 2.2, "Medical") :: Purchase(2, 3.2, "Health") :: Nil


    val rdd1 = sc.parallelize(personArr, 2)

    val rdd2 = sc.parallelize(personPurchaseArr, 2)


    rdd1.zip(rdd2).mapPartitions(iter => iter.map({ x =>

      val pjtype = x._2.JobCategory match {
        case "IT" => "Techie"
        case _ => "Non Techie"
      }

      (x._1.personID, x._1.personName, x._2.tranAmt, pjtype)
    }
    )).foreach(println)

  }
}

case class Person1(val personID: Int, val personName: String)

case class Purchase(val personId: Int, val tranAmt: Double, val JobCategory: String)

