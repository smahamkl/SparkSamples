
package org.ndc.datafaker.schema

import org.ndc.datafaker.YamlParser.YamlParserProtocol
import org.ndc.datafaker.YamlParser.YamlParserProtocol
import org.ndc.datafaker.schema.table.SchemaTable

case class Schema(tables: List[SchemaTable])

object SchemaProtocol extends YamlParserProtocol {

  import org.ndc.datafaker.schema.table.SchemaTableProtocol._

  implicit val formatSchema = yamlFormat1(Schema.apply)

}
