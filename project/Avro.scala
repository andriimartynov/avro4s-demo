import sbt._

object Avro {
  lazy val dependencies: Seq[ModuleID] = Seq(
    "com.sksamuel.avro4s" %% "avro4s-core" % "2.0.4"
  )

}
