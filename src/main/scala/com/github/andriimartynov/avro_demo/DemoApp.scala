package com.github.andriimartynov.avro_demo

import java.io.ByteArrayOutputStream

import com.sksamuel.avro4s.{AvroInputStream, AvroOutputStream, AvroSchema, Decoder, Encoder, FromRecord, SchemaFor, ToRecord}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.{GenericData, GenericRecord}
import shapeless.Generic

object DemoApp extends App {
  sealed trait DemoEvent

  @deprecated("test", "test")
  case class DemoV1(value: Int) extends DemoEvent

  case class DemoV2(value: String) extends DemoEvent

  implicit val DemoV1SchemaFor: SchemaFor[DemoV1] = SchemaFor.const[DemoV1](SchemaBuilder
    .record("DemoV1").namespace("com.github.andriimartynov.avro_demo.DemoApp")
    .fields()
    .name("value").`type`.intType().noDefault()
    .endRecord())

  implicit object DemoV1Decoder extends Decoder[DemoV1] {

    override def decode(value: Any, schema: Schema): DemoV1 = {
      val record = value.asInstanceOf[GenericRecord]
      DemoV1(record.get("value").toString.toInt)
    }
  }

  implicit object DemoV1Encoder extends Encoder[DemoV1] {

    val schemaFor = DemoV1SchemaFor

    override def encode(t: DemoV1, schema: Schema): AnyRef = {
      val record = new GenericData.Record(schema)
      record.put("value", t.value)
      record
    }
  }

  implicit val gameEventGeneric = Generic[DemoEvent]
  implicit val schemaFor: SchemaFor[DemoEvent] = SchemaFor.genCoproduct[DemoEvent, gameEventGeneric.Repr]
  implicit val toRecord: ToRecord[DemoEvent] = ToRecord[DemoEvent]
  implicit val fromRecord: FromRecord[DemoEvent] = FromRecord[DemoEvent]

  val event = DemoV1(2)
  val bytes = toBinary(event)
  val dEvent = fromBinary(bytes)
  println(dEvent)

  def toBinary(event: DemoEvent): Array[Byte]  = {
    val output = new ByteArrayOutputStream
    val avro = AvroOutputStream.data[DemoEvent].to(output).build(AvroSchema[DemoEvent])
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
