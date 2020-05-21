package org.scalavariants

class Animal{
  def someMethod = this.getClass.getCanonicalName
}


class Dog extends Animal

class Puppy extends Animal

class PuppyChild extends Puppy{
  def someOtherMethod: String = "puppy child method " + this.getClass.getCanonicalName
}

class completelyDifferentObject
//upper bound
class PuppyCarer {
  def display[T <: Puppy](t: T) {
    println(t)
  }
}
//lower bound (less restrictive)
class AnimalCarer {
  def display[T >: Dog](t: T) {
    //println(t.asInstanceOf[Animal].someMethod)

    println(t.asInstanceOf[PuppyChild].someOtherMethod)
  }
}

object Bounds extends App {

  val animal = new Animal
  val dog = new Dog
  val puppy = new Puppy
  val puppyChild = new PuppyChild

  val puppyCarer = new PuppyCarer
  puppyCarer.display(puppy)
  puppyCarer.display(puppyChild)

val animalCarer = new AnimalCarer
  //animalCarer.display(animal)
  animalCarer.display(puppyChild)
/*
  //this would throw run time error
  val someChild = new completelyDifferentObject
  animalCarer.display(someChild)*/
}


