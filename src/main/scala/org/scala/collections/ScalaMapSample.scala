package org.scala.collections

object ScalaMapSample {

  def main(args: Array[String]) {

    import scala.collection.mutable.HashMap
    var map = HashMap.empty[Int, Int]

    def getDivSum(num: Int): Int = {
      if (!(map contains num))
        map(num) = (for { a <- 1 until num; if (num % a == 0) } yield a).toList.sum

      map(num)
    }

    println(getDivSum(6))

   def isAmicable(num: Int): Boolean = {
      val fst = getDivSum(num)
      num == getDivSum(fst) && num != fst
    }

    val result = (for {
      i <- 1 to 10000;
      sum = getDivSum(i);
      if (isAmicable(sum))
    } yield sum).toSet

    println(result.sum)
  }

}
