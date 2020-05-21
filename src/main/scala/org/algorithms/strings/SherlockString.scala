package org.algorithms.strings

/*
https://www.hackerrank.com/challenges/sherlock-and-valid-string/problem

Sherlock considers a string to be valid if all characters of the string appear the same number of times. It is also valid if he can remove just

character at index in the string, and the remaining characters will occur the same number of times. Given a string

, determine if it is valid. If so, return YES, otherwise return NO. For example, if s=abc, it is a valid string because frequencies are
 {a:1, b:1,c:1} . So is s=abcc (after removing c)

because we can remove one occurence of c and have one occurence of each character in the remaining string.

Function Description
Complete the isValid function in the editor below. It should return either the string YES or the string NO.
isValid has the following parameter(s):
s: a string
Input Format
A single string

Output Format
Print YES if string

is valid, otherwise, print NO.
 */

import scala.collection.mutable.HashMap
import scala.collection.immutable.ListMap


object SherlockString {

  def isValid(s: String): String = {
    try {
      val hashMap1: HashMap[String, Int] = HashMap()

      s.map(ch => {
        if (hashMap1.contains(String.valueOf(ch))) {
          hashMap1.update(String.valueOf(ch), hashMap1(String.valueOf(ch)) + 1)
        } else
          hashMap1 += String.valueOf(ch) -> 1
      })

      val a = ListMap(hashMap1.toList.map(x => (x._2, 1)).groupBy(_._1).map { case (key, value) => key -> (value.map(_._2).sum) }.toSeq.sortBy(_._1): _*)

      println(a)

      if (a.size == 1)
        "YES"
      else if (a.size == 2 && ((a.keySet.toList(1) - a.keySet.toList(0)) == 1) && a(a.keySet.toList(1)) == 1)
        "YES"
      else if (a.size == 2 && a(1) == 1)
        "YES"
      else
        "NO"
    }
    catch {
      case ex: Exception => "NO"
    }
  }

  def main(args: Array[String]): Unit = {

    var s = "aaabbbcccdddeeee"
    println(isValid(s))

    s = "aabcd"
    println(isValid(s))

    s = "aaabbbcccd"
    println(isValid(s))

    s = "aabbcd"
    println(isValid(s))

    s = "aabbccddeefghi"
    println(isValid(s))


  }

}
