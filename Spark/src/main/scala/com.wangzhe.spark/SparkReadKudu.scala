package com.wangzhe.spark

import java.util.Properties

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

object SparkReadKudu {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder().
      master("local[2]").appName("read_kudu").getOrCreate()
    val df = session.read.format("org.apache.kudu.spark.kudu").load()
    df.show(20)



    val properties = new Properties()
    properties.setProperty("user","root")
    properties.setProperty("password","123")
    //session.read.jdbc("jdbc:mysql://localhost:3306/test", "student", properties)
    def setNotNull(df: DataFrame, columns: Seq[String]) : DataFrame = {
      val schema = df.schema
      // Modify [[StructField] for the specified columns.
      val newSchema = StructType(schema.map {
        case StructField(c, t, _, m) if columns.contains(c) => StructField(c, t, nullable = false, m)
        case y: StructField => y
      })
      // Apply new schema to the DataFrame
      df.sqlContext.createDataFrame(df.rdd, newSchema)
    }
//    val sftmta_time = df
//      .withColumn("REPORT_TIME", to_timestamp($"REPORT_TIME", "MM/dd/yyyy HH:mm:ss"))
//    val sftmta_prep = setNotNull(sftmta_time, Seq("REPORT_TIME", "VEHICLE_TAG"))
//    sftmta_prep.printSchema
//    sftmta_prep.createOrReplaceTempView("sftmta_prep")
      session.sql("SELECT REPORT_TIME, VEHICLE_TAG, PREDICTABLE FROM kudu_table where PREDICTABLE = 1").show()
//    session.sql("SELECT * FROM sftmta_prep LIMIT 5").show()
//
//
//    // Delete the table if it already exists.
//    if(kuduContext.tableExists("sfmta_kudu")) {
//      kuduContext.deleteTable("sfmta_kudu")
//    }

//    kuduContext.createTable("sfmta_kudu", sftmta_prep.schema,
//      /* primary key */ Seq("REPORT_TIME", "VEHICLE_TAG"),
//      new CreateTableOptions()
//        .setNumReplicas(3)
//        .addHashPartitions(List("VEHICLE_TAG").asJava, 4))
  }
}
