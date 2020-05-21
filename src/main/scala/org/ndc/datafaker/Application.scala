
package org.ndc.datafaker

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Application extends App {

  Logger.getLogger("org.apache.spark.SparkContext").setLevel(Level.WARN)

  val parsedArgs = ArgsParser.validateArgs(ArgsParser.parseArgs(args.toList))
  val conf = new SparkConf()
    .set("spark.ui.showConsoleProgress", "true")
    .setAppName("data-faker")
    .setMaster("local")
  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    //.enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("OFF")

  //spark.sql(s"create database if not exists ${parsedArgs("database")}")

  //val schema = YamlParser.parseSchemaFromFile(parsedArgs("file"))
  val schema = YamlParser.parseSchemaFromFile("/home/sivam/GITREPO/spark-pika/src/main/resources/example.yaml")
 // val dataGenerator = new DataGenerator(spark, parsedArgs("database"))
  val dataGenerator = new DataGenerator(spark, "")

  dataGenerator.generateAndWriteDataFromSchema(schema)
}
