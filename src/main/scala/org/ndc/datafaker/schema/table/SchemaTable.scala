
package org.ndc.datafaker.schema.table

import org.ndc.datafaker.YamlParser.YamlParserProtocol
import org.ndc.datafaker.YamlParser.YamlParserProtocol
import org.ndc.datafaker.schema.table.columns.SchemaColumn

case class SchemaTable(name: String, rows: Long, columns: List[SchemaColumn], partitions: Option[List[String]])

object SchemaTableProtocol extends YamlParserProtocol {

  import org.ndc.datafaker.schema.table.columns.SchemaColumnProtocol._

  implicit val formatSchemaTable = yamlFormat4(SchemaTable.apply)

}