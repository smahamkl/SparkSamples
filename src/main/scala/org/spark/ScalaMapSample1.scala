package org.spark

import scala.collection.mutable

object ScalaMapSample1 {

  def main(args:Array[String]):Unit = {

    //val tasks = Map(1 -> "Book movie tickets",2 -> "Go shopping")
    val tasks = mutable.Map[Int,String]()

    tasks += (1 -> "Book a movie ticket")

    tasks += (2 -> "Go shopping")

    var subjects = Map(1 -> "DS",2 -> "C++",3 -> "Java")

    println(subjects.keys)

    println(subjects.values)

    println(subjects.isEmpty)

    subjects = Map(1 -> "DS",2 -> "C++",3 -> "Java")

    val newSubjects = Map(3 -> "Scala", 4 -> "c#")

    val listOfSubjects = subjects ++ newSubjects

    println(listOfSubjects)

    subjects = Map(1 -> "DS",2 -> "C++",3 -> "Java")

    println(subjects.filterKeys(x => x%2 == 0))

  }

}
