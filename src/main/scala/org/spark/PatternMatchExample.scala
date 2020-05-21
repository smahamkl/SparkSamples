package org.spark

object PatternMatchExample extends App{

  val input = "somefield,string,null"
  //Use '.r' to create a Regex Object
  val pattern = """^(.*),(.*),(.*)$""".r
  val result = pattern.findFirstMatchIn(input)

  result match {
    case Some(x) =>
      val field = x.group(1)
      val fieldType = x.group(2)
      val nullInfo = x.group(3)
      println(s"field -> $field, fieldType -> $fieldType," +
        s" nullInfo -> $nullInfo")

    case None => println("Not match found")
  }

}
