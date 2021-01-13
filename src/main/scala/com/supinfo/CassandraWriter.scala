package com.supinfo

import org.apache.spark.sql.cassandra.DataFrameWriterWrapper
import org.apache.spark.sql.{DataFrame, SaveMode}

object CassandraWriter {
  private val CASSANDRA_HOST = "cassandra.supinfo.lan"
  private val CASSANDRA_USER = "cassandra"
  private val CASSANDRA_PASSWORD = "cassandra"

  implicit class DataFrameCassandraWriter(df: DataFrame) {
    def saveToCassandra(table: String): Unit = df.write
      .cassandraFormat(table, "supinfodwh")
      .option("spark.cassandra.connection.host", CASSANDRA_HOST)
      .option("spark.cassandra.auth.username", CASSANDRA_USER)
      .option("spark.cassandra.auth.password", CASSANDRA_PASSWORD)
      .option("confirm.truncate", true)
      .mode(SaveMode.Overwrite)
      .save()
  }

}
