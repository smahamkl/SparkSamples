
package org.ndc.datafaker

import scala.io.Source
import java.sql.{Date, Timestamp}

import net.jcazevedo.moultingyaml.DefaultYamlProtocol
import org.joda.time.DateTimeZone
import org.ndc.datafaker.schema.Schema

object YamlParser {

  def parseSchemaFromFile(fileName: String): Schema = {
    import org.ndc.datafaker.schema.SchemaProtocol._
    import net.jcazevedo.moultingyaml._

    val yaml = Source.fromFile(fileName).mkString.stripMargin.parseYaml

    yaml.convertTo[Schema]
  }

  /**
    * Provides additional formats for unsupported types in DefaultYamlProtocol
    */
  object YamlParserProtocol extends YamlParserProtocol
  trait YamlParserProtocol extends DefaultYamlProtocol {

    import net.jcazevedo.moultingyaml._

    implicit object TimestampFormat extends YamlFormat[Timestamp] {
      override def read(yaml: YamlValue): Timestamp = {
        yaml match {
          case YamlDate(d) => Timestamp.valueOf(d.toDateTime(DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss"))
          case _ => deserializationError("error parsing Timestamp")
        }
      }

      override def write(obj: Timestamp): YamlValue = ???
    }

    implicit object DateFormat extends YamlFormat[Date] {

      override def read(yaml: YamlValue): Date = {
        yaml match {
          case YamlDate(d) => Date.valueOf(d.toString("yyyy-MM-dd"))
          case _ => deserializationError("error parsing Date")
        }
      }

      override def write(obj: Date): YamlValue = ???

    }

  }

}
