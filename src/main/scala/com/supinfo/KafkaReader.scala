package com.supinfo

import org.apache.spark.sql.functions.{callUDF, from_json}
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaReader {
  private val KAFKA_BROKER_URL = "kafka.supinfo.lan:9092"

  implicit class SparkSessionKafkaReader(spark: SparkSession) {
    spark.udf.register("deserialize", (bytes: Array[Byte]) => AvroHelper.deserializer.deserialize(bytes))

    def readKafka(topic: String): DataFrame = {
      import spark.implicits._
      val df = spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER_URL)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("auto.offset.reset", "earliest")
        .load()

      val jsonDf = df.select(callUDF("deserialize", 'value) as 'raw_value)
      val schema = {
        val rawSchema = AvroHelper.getSchemaBySubject(topic + "-value")
        AvroHelper.avroSchemaToSparkSchema(rawSchema)
      }
      jsonDf
        .select(from_json('raw_value, schema.dataType) as 'parsed_value)
        .select($"parsed_value.*")
    }
  }

}
