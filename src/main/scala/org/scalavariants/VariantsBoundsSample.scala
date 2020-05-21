package org.scalavariants


class Array[+X] {
  def add[Y >: X](elem: Y): Array[Y] = new Array[Y] {
    override def first: Y = elem
    override def retrieve: Array[Y] = Array.this
    override def toString() = elem.toString() + "\n" + Array.this.toString()
  }
  def first: X = sys.error("No elements in the Array")
  def retrieve: Array[X] = sys.error("Array is empty")
  override def toString() = ""
}
object ArrayTest extends App {

  var a: Array[Any] = new Array().add("Hi");
  a = a.add(new Object())
  a = a.add(56)
  a = a.add(67.89)
  println("Array elements added are: " + a)

}

