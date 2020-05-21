package org.spark

object HigherOrderFuncSample1 {


  def main(args: Array[String]) ={

    def apply(f:Int => String, v:Int) = f(v)

    val d = new Decorator("[", "]")

    println(apply(d.layout, 6))

    val sum = (i:Int, j:Int) => i + j

    println(sum(1,2))

  }

}
class Decorator(left:String, right:String)  {
  def layout[A](x: A) = left + x.toString() + right
}