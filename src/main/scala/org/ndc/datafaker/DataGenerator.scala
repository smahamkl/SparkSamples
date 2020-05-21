
package org.ndc.datafaker

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.ndc.datafaker.schema.Schema

class DataGenerator(spark: SparkSession, database: String) extends Serializable {

  private val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  def generateAndWriteDataFromSchema(schema: Schema) {
    for (table <- schema.tables) {
      logger.info(s"generating and writing ${table.rows} rows for ${table.name}")

      val partitions = table.partitions.getOrElse(List.empty[String])
      val dataFrame = table.columns.foldLeft(spark.range(table.rows).toDF("rowID"))((a, b) => {
          a.withColumn(b.name, b.column())
      }).drop("rowID")

      //dataFrame.write.mode(SaveMode.Overwrite).partitionBy(partitions: _*).saveAsTable(s"$database.${table.name}")
      dataFrame.write.mode(SaveMode.Overwrite).format("csv").partitionBy(partitions: _*).save(s"file:/home/sivam/GITREPO/dataout/out/${table.name}")
      
      logger.info(s"${table.name} - complete")
    }
  }

}
