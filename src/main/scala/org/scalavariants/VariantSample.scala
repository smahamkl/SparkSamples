package org.scalavariants

///Simple example for Scala Covariance, Contravariance and Invariance

object VariantSample {

  class Animal {}

  class Mammal extends Animal {}

  class Dog extends Mammal {}

  /*Box1 is covariant in T
Box[Dog] can be used for Box[Mammal] not Box[Animal]
Only subtypes of Mammal are ok*/
  class Box1[+T] {}

  /*Box2 is contravariant in T
  Box[Animal] can be used for Box[Mammal] not Box[Dog]
  Only supertypes of Mammal are ok*/
  class Box2[-T] {}

  /*Box3 is invariant in T
  Only Mammal is ok*/
  class Box3[T] {}

  def method1(box:Box1[Mammal]){}
  def method2(box:Box2[Mammal]){}
  def method3(box:Box3[Mammal]){}


  def main(args: Array[String]): Unit = {

    /*covariance*/
    //method1(new Box1[Animal]) //compile fails
    method1(new Box1[Mammal])
    method1(new Box1[Dog])

    /*contravariance*/
    method2(new Box2[Animal])
    method2(new Box2[Mammal])
    //method2(new Box2[Dog]) //compile fails

    /*invariance*/
    //method3(new Box3[Animal]) //compile fails
    method3(new Box3[Mammal])
    //method3(new Box3[Dog]) //compile fails
  }

}
