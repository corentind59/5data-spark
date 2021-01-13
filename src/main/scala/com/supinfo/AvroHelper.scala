package com.supinfo

import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.avro.SchemaConverters

object AvroHelper {
  private val SCHEMA_REGISTRY_URL = "http://kafka.supinfo.lan:8081"
  private val schemaRegistryClient = new CachedSchemaRegistryClient(SCHEMA_REGISTRY_URL, 128)
  val deserializer = new AvroDeserializer(schemaRegistryClient)

  def getSchemaBySubject(subject: String): String =
    schemaRegistryClient.getLatestSchemaMetadata(subject).getSchema

  def avroSchemaToSparkSchema(avroSchema: String): SchemaConverters.SchemaType =
    SchemaConverters.toSqlType(new Schema.Parser().parse(avroSchema))

  class AvroDeserializer extends AbstractKafkaAvroDeserializer {
    def this(client: SchemaRegistryClient) {
      this()
      this.schemaRegistry = client
    }

    override def deserialize(bytes: Array[Byte]): String = {
      val value = super.deserialize(bytes)
      value match {
        case str: String =>
          str
        case _ =>
          val genericRecord = value.asInstanceOf[GenericRecord]
          genericRecord.toString
      }
    }
  }
}
