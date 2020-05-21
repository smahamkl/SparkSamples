package org.scala.collections


object LinkedListSample {

  sealed trait ConsInterface[+A] {
    def value: A

    def next: ConsInterface[A]
  }

  //this is the tail end of the linked list which is the null object
  //There should only be one NilCons value in the world, define it as a singleton by declaring it as an object
  object NilCons extends ConsInterface[Nothing] {
    def value = throw new NoSuchElementException("head of empty list")

    def next = throw new UnsupportedOperationException("tail of empty list")
  }

  case class Cons[+A](val value: A, val next: ConsInterface[A]) extends ConsInterface[A] {
    override def toString = s"head: $value, next: $next"
  }

  def main(args: Array[String]): Unit = {
    val c1 = Cons('a', NilCons)
    val c2 = Cons('b', c1)
    val c3 = Cons('c', c2)

    println(c1)
    println(c2)
    println(c3)
  }

}
