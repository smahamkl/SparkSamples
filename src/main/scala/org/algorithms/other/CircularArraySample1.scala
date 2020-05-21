package org.algorithms.other

object CircularArraySample1 extends App {

  var a = Array(2, -1, 1, -2, -2)

  //a = Array(1, -2)

  //a = Array(-1, 2)

  // println(a.length)

  //println(isLinkingToFirstElement(2, 4, 3))

  var forwardLoop = new Array[Int](a.length)
  var backLoop = a.clone()
  //convert relative positions to absolute positions
  for (i <- 0 to a.length - 1) {
    if (a(i) > 0) {
      forwardLoop(i) = (i + a(i)) % a.length
      backLoop(i) = -1
    }
    else if (a(i) < 0) {
      forwardLoop(i) = -1
      if (a(i) + i >= 0)
        backLoop(i) = a(i) + i
      else
        backLoop(i) = a.length - (Math.abs(a(i)) - i)
    }
    else {
      backLoop(i) = -1
      forwardLoop(i) = -1
    }
  }

  for (i <- 0 to forwardLoop.length - 1) {
    print(forwardLoop(i) + " ")
  }
  println()
  for (i <- 0 to backLoop.length - 1) {
    print(backLoop(i) + " ")
  }
  println()

  var j = 0
  var i = Integer.MAX_VALUE
  var cycles = 0
  var totelements = 0

  while (cycles < forwardLoop.length) {
    if (i == forwardLoop(j) && cycles > 1 && totelements > 1)
      println("cycle detected")


    if (forwardLoop(j) > 0) {
      j = forwardLoop(j)
      i = Math.min(i, j)
      totelements += 1
    }
    else if(forwardLoop(j) == 0 && totelements > 1)
      j = forwardLoop(j)
    else
      j = j + 1

    cycles += 1
  }

  j = 0
  i = Integer.MAX_VALUE
  cycles = 0
  totelements = 0

  while (cycles < backLoop.length) {

    if (i == backLoop(j) && cycles > 1 && totelements > 1)
      println("cycle detected")


    if (backLoop(j) > 0) {
      j = backLoop(j)
      i = Math.min(i, j)
      totelements += 1
    }
    else if(backLoop(j) == 0 && totelements > 1)
      j = backLoop(j)
    else
      j = j + 1
    cycles += 1
  }
}
