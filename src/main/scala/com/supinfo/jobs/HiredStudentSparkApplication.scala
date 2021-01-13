package com.supinfo.jobs

import com.supinfo.CassandraWriter.DataFrameCassandraWriter
import com.supinfo.KafkaReader.SparkSessionKafkaReader
import com.supinfo.KafkaTopics._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object HiredStudentSparkApplication {
  def main(args: Array[String]): Unit = {

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Supinfo DWH - Hired Supinfo students profiling")
      .getOrCreate()
    import spark.implicits._

    // Load datasets
    val students = spark.readKafka(KAFKA_TOPIC_STUDENT)
      .withColumnRenamed("id", "student_id")
    val notes = spark.readKafka(KAFKA_TOPIC_NOTE)
    val campuses = spark.readKafka(KAFKA_TOPIC_CAMPUS)
      .withColumnRenamed("id", "campus_id")
      .withColumnRenamed("region", "campus_region")
      .withColumnRenamed("city", "campus_city")
    val companies = spark.readKafka(KAFKA_TOPIC_COMPANY)
      .withColumnRenamed("id", "company_id")
    val proContracts = spark.readKafka(KAFKA_TOPIC_PRO_CONTRACT)
      .withColumnRenamed("begin_date", "pro_contract_begin_date")
    val hirings = spark.readKafka(KAFKA_TOPIC_HIRING)
      .withColumnRenamed("begin_date", "contract_begin_date")

    // Denormalize and aggregate data
    val dnmlzdProContracts = proContracts
      .join(companies, "company_id")
      .withColumnRenamed("name", "pro_contract_company_name")
    val dnmlzdHirings = hirings
      .join(companies, "company_id")
      .withColumnRenamed("name", "contract_company_name")

    val formerStudents = notes
      .groupBy('student_id, 'promotion)
      .agg(avg('note) as 'global_avg_note)
      .join(dnmlzdHirings, Seq("student_id"), "right")
      .join(students, "student_id")
      .join(dnmlzdProContracts, Seq("student_id"), "left")
      .join(campuses, "campus_id")
      .drop("campus_id", "student_id", "company_id")

    // Write result to Cassandra table
    formerStudents.saveToCassandra("hired_students_by_company")

    spark.stop()
  }
}
