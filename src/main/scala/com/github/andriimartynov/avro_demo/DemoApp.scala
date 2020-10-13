package com.github.andriimartynov.avro_demo

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, FromRecord, SchemaFor, ToRecord}

object DemoApp extends App {
  sealed trait DemoEvent

  @deprecated("test", "test")
  case class DemoV1(value: Int) extends DemoEvent

  case class DemoV2(value: String) extends DemoEvent

  implicit val toRecord: ToRecord[DemoEvent] = ToRecord[DemoEvent]
  implicit val fromRecord: FromRecord[DemoEvent] = FromRecord[DemoEvent]

  val event = DemoV1(2)
  val bytes = toBinary(event)
  val dEvent = fromBinary(bytes)
  println(dEvent)

  def toBinary(event: DemoEvent): Array[Byte]  = {
    val output = new ByteArrayOutputStream
    val avro = AvroOutputStream.data[DemoEvent].to(output).build()
    avro.write(event)
    avro.close()
    output.toByteArray
  }

  def fromBinary(bytes: Array[Byte]): DemoEvent =
    AvroInputStream
      .data[DemoEvent]
      .from(bytes)
      .build(AvroSchema[DemoEvent])
      .iterator
      .next()

}
