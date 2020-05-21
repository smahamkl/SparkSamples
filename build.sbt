name := "spark-data-faker"

version := "0.0.1"

scalaVersion := "2.11.11"

sparkVersion := "2.4.4"

mainClass in Compile := Some("org.ndc.datafaker.Application")

//libraryDependencies += "mrpowers" % "spark-daria" % "2.3.1_0.24.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.4.4" % "provided"
//libraryDependencies += "MrPowers" % "spark-fast-tests" % "2.3.1_0.15.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "net.jcazevedo" %% "moultingyaml" % "0.4.0"
libraryDependencies += "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
assemblyJarName in assembly := s"${name.value}_2.11-${sparkVersion.value}_${version.value}.jar"

//assemblyShadeRules in assembly := Seq(
//  ShadeRule.rename("org.ndc.mrpowers.spark.daria.**" -> "shadedSparkDariaForSparkPika.@1").inAll
//)

assemblyMergeStrategy in assembly <<= (assemblyMergeStrategy in assembly) { (old) =>
{
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
}