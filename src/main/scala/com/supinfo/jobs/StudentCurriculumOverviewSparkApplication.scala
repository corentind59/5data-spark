package com.supinfo.jobs

import com.supinfo.CassandraWriter.DataFrameCassandraWriter
import com.supinfo.KafkaReader.SparkSessionKafkaReader
import com.supinfo.KafkaTopics._
import org.apache.spark.sql.SparkSession

object StudentCurriculumOverviewSparkApplication {
  def main(args: Array[String]): Unit = {

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Supinfo DWH - Gather students' all notes and informations")
      .getOrCreate()

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
      .withColumnRenamed("name", "pro_contract_company_name")
    val proContracts = spark.readKafka(KAFKA_TOPIC_PRO_CONTRACT)
      .withColumnRenamed("begin_date", "pro_contract_begin_date")

    // Denormalize and aggregate data
    val dnmlzdProContracts = proContracts
      .join(companies, "company_id")

    students.show(false)

    val studentsNotes = students
      .join(notes, "student_id")
      .join(dnmlzdProContracts, Seq("student_id"), "left")
      .join(campuses, "campus_id")
      .drop("campus_id", "student_id", "company_id")

    // Write result to Cassandra table
    studentsNotes.saveToCassandra("student_notes_by_course_and_promotion")

    spark.stop()
  }

}
