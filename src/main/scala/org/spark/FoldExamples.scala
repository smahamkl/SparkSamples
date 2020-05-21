package org.spark

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object FoldExamples {

  def main(args: Array[String]) = {
    Logger.getLogger("org.apache").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
    val sc = new SparkContext(new SparkConf().setAppName("Fold Left/Right Examples").setMaster("local"))

    val fooList = Foo("Hugh Jass", 25, 'male) ::
      Foo("Biggus Dickus", 43, 'male) ::
      Foo("Incontinentia Buttocks", 37, 'female) ::
      Nil

    val stringList = fooList.foldLeft(List[String]()) { (z, f) =>
      val title = f.sex match {
        case 'male => "Mr."
        case 'female => "Ms."
      }
      z :+ s"$title ${f.name}, ${f.age}"
    }.foreach(println)

  }

}

case class Foo(val name: String, val age: Int, val sex: Symbol)


