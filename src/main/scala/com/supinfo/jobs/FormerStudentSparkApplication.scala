package com.supinfo.jobs

import com.supinfo.CassandraWriter.DataFrameCassandraWriter
import com.supinfo.KafkaReader.SparkSessionKafkaReader
import com.supinfo.KafkaTopics._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object FormerStudentSparkApplication {
  def main(args: Array[String]): Unit = {

    // Create Spark session
    val spark = SparkSession.builder()
      .appName("Supinfo DWH - Former Supinfo students profiling")
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
      .withColumnRenamed("name", "pro_contract_company_name")
    val proContracts = spark.readKafka(KAFKA_TOPIC_PRO_CONTRACT)
      .withColumnRenamed("begin_date", "pro_contract_begin_date")
    val concurrents = spark.readKafka(KAFKA_TOPIC_CONCURRENT)
      .withColumnRenamed("id", "concurrent_id")
      .withColumnRenamed("name", "concurrent_school_name")
    val schoolLeaves = spark.readKafka(KAFKA_TOPIC_SCHOOL_LEAVE)

    // Denormalize and aggregate data
    val dnmlzdProContracts = proContracts
      .join(companies, "company_id")
    val dnmlzdSchoolLeaves = schoolLeaves
      .join(concurrents, "concurrent_id")

    val formerStudents = notes
      .groupBy('student_id, 'promotion)
      .agg(avg('note) as 'global_avg_note)
      .join(dnmlzdSchoolLeaves, Seq("student_id"), "right")
      .join(students, "student_id")
      .join(dnmlzdProContracts, Seq("student_id"), "left")
      .join(campuses, "campus_id")
      .drop("campus_id", "student_id", "company_id", "concurrent_id")

    // Write result to Cassandra table
    formerStudents.saveToCassandra("former_students_by_concurrents")

    spark.stop()
  }
}
