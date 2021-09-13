package com.wangzhe.flink.flink

import org.apache.flink.api.common.io.RichOutputFormat
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConverters._
import scala.collection.mutable


class MongoOutputFormat(val uri: String, val database: String, val collection: String) extends RichOutputFormat[Map[String, Any]]{
  val KEY_URI = "uri"
  val KEY_DATABASE = "database"
  val KEY_COLLECTION = "collection"

  val BATCH_SIZE = 2000

  private var mongoClient: MongoClient = null
  private var mongoCollection: MongoCollection[Document] = null
  private val newRecords: mutable.MutableList[Document] = mutable.MutableList()

  override def configure(parameters: Configuration): Unit = {

  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {
    println(this + " start open!")
    mongoClient = new MongoClient(new MongoClientURI(uri))
    mongoCollection = mongoClient.getDatabase(database).getCollection(collection)
  }

  override def writeRecord(record: Map[String, Any]): Unit = {
    val document = new Document
    for((key, value) <- record){
      document.append(key, value)
    }
    newRecords += document

    if(newRecords.size >= BATCH_SIZE) {
      flush()
    }
  }

  def flush(): Unit = {
      mongoCollection.insertMany(newRecords.asJava)
      newRecords.clear()
  }

  override def close(): Unit = {
    if(!newRecords.isEmpty){
      flush()
    }
    //mongoClient.close()
    //mongoClient = null
  }
}

object MongoOutputFormat {

  def apply(config: Map[String, String]): MongoOutputFormat = {
    val mongoOutputFormat = new MongoOutputFormat(
      config("uri"), config("database"), config("collection"))
    mongoOutputFormat
  }
}
