package org.algorithms.strings

import scala.collection.mutable
import scala.math._
import scala.reflect.internal.util.HashSet
import scala.util.Random

class Circle {

  import Circle._

  def area(r: Double): Double = calculateArea(r)


  def sum(f: Int => Int, a: Int, b: Int): Int =
    if (a > b) 0
    else f(a) + sum(f, a + 1, b)

  def id(x: Int): Int = x

  def cube(x: Int): Int = x * x * x

  def factorial(x: Int): Int = {
    if (x == 1)
      1
    else
      x * factorial(x - 1)
  }

  def sumInts(a: Int, b: Int) = sum(id, a, b)

  def sumCubes(a: Int, b: Int) = sum(cube, a, b)

  def sumFactorials(a: Int, b: Int) = sum(factorial, a, b)

}

object Circle {

  private def calculateArea(r: Double): Double = (22 / 7) * pow(r, 2)
}

object CustomerID {

  def apply(name: String) = s"$name--${Random.nextLong}"

  def unapply(customerID: String): Option[String] = {
    val stringArray: Array[String] = customerID.split("--")
    if (stringArray.tail.nonEmpty) Some(stringArray.head) else None
  }
}


object ArrayTest extends App {
  val c = new Circle
  println(c.area(10))

  val c1 = CustomerID("Sivaprasad")
  println(c1)

  val customerIDObj = c1
  println(customerIDObj)

  println(c.sumFactorials(3, 4))


  var s = "aaabbbcccdddeeee"
  println(isSherlockString(s))
  val strMap = mutable.HashMap[Character, Int]()

  strMap('A') = 2
  strMap('B') = 2

  def isSherlockString(s: String): Boolean = {

    val a = s.map(char => (char, 1)).groupBy(_._1).map { case (key, value) => key -> value.map(x => (x._2)).sum }

    println(a)

    // val valueSet:Set[Int] = a.map(x => x._2).toSet
    for ((k, v) <- a) printf("key: %s, value: %s\n", k, v)

    return true

  }

  def sum1(i: Int, j: Int): Int = sum1(id, i, j)

  def sum1(f: Int => Int, i: Int, j: Int): Int = {
    return f(i) + sum1(i + 1, j)
  }

  def id(i: Int): Int = i
}


/*object Sample1 {

  trait Animal{
    def speak: String
  }

  case class Dog extends Animal{
    def speak = "Bow"
  }
  case class Cat extends Animal{
    def speak = "Meow"
  }


  def main(args:Array[String]) = {

    val iList = List(2, 7, 9, 8, 10);
    val iDoubled = iList.filter(_ % 2 == 0).map(_ + 2)

    println(iDoubled)

  }

}*/
