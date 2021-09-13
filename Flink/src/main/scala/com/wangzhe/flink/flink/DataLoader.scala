package com.wangzhe.flink.flink

object DataLoader {
  val MOVIE_DATA_PATH = "/home/wangzhe/IdeaProjects/ScalaProgramming/RecommendSystem/src/main/resources/movies.csv"
  val RATING_DATA_PATH = "/home/wangzhe/IdeaProjects/ScalaProgramming/RecommendSystem/src/main/resources/ratings.csv"
  val TAG_DATA_PATH = "/home/wangzhe/IdeaProjects/ScalaProgramming/RecommendSystem/src/main/resources/tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"

  val ES_MOVIE_INDEX = "Movie"

  /**
    * Movie数据集，数据集字段通过分割
    *
    * 151^                          电影的ID
    * Rob Roy (1995)^               电影的名称
    * In the highlands ....^        电影的描述
    * 139 minutes^                  电影的时长
    * August 26, 1997^              电影的发行日期
    * 1995^                         电影的拍摄日期
    * English ^                     电影的语言
    * Action|Drama|Romance|War ^    电影的类型
    * Liam Neeson|Jessica Lange...  电影的演员
    * Michael Caton-Jones           电影的导演
    * * * tag1|tag2|tag3|....           电影的Tag
    * */

  case class Movie(val mid: Int, val name: String, val descri: String, val timelong: String, val issue: String,
                   val shoot: String, val language: String, val genres: String, val actors: String, val directors: String)

  /**
    * Rating数据集，用户对于电影的评分数据集，用，分割
    *
    * 1,           用户的ID
    * 31,          电影的ID
    * 2.5,         用户对于电影的评分
    * 1260759144   用户对于电影评分的时间
    */
  case class Rating(val uid: Int, val mid: Int, val score: Double, val timestamp: Int)

  /**
    * Tag数据集，用户对于电影的标签数据集，用，分割
    *
    * 15,          用户的ID
    * 1955,        电影的ID
    * dentist,     标签的具体内容
    * 1193435061   用户对于电影打标签的时间
    */
  case class Tag(val uid: Int, val mid: Int, val tag: String, val timestamp: Int)


  def main(args: Array[String]): Unit = {
    //引入隐式转换
    //implicit val tpeInfo = BasicTypeInfo.SHORT_TYPE_INFO
    //引入隐式转换
    implicit val tupleTypeInfo =
    new TupleTypeInfo[org.apache.flink.api.java.tuple.Tuple2[String, String]](
      BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO)
    //引入隐式转换
    import org.apache.flink.api.scala._

    val environment = ExecutionEnvironment.getExecutionEnvironment
    val movieDataset = environment.readCsvFile[Movie](MOVIE_DATA_PATH, fieldDelimiter = "^")
    val tagDataset = environment.readCsvFile[Tag](TAG_DATA_PATH, fieldDelimiter = ",")

    val movieWithTagsData = movieDataset.leftOuterJoin(tagDataset)
      .where("mid").equalTo("mid") {
      (d1, d2) => {
        val tag = if (d2 != null) d2.tag else ""
        (d1.mid, d1.name, tag)
      }
    }

    val movieDataToMapSet = movieDataset.map(movie => {
      val map = Map("mid" -> movie.mid, "name" -> movie.name, "descri" -> movie.descri,
        "timelong" -> movie.timelong, "issue" -> movie.issue, "shoot" -> movie.shoot,
        "language" -> movie.language, "genres" -> movie.genres, "actors" -> movie.actors,
        "directors" -> movie.directors)
      map
    })

    saveToMongo(movieDataToMapSet, "Movie")


    environment.execute()

  }

  def saveToMongo(dataset: DataSet[Map[String, Any]], collection: String,
                  writeMode: String = "overwrite"): Unit = {
    val config = Map("uri" -> "mongodb://node3:27017",
      "database" -> "recommender", "collection" -> collection)

    if (writeMode.equals("overwrite")) {
      //新建一个到MongoDB的连接
      val mongoClient = new MongoClient(new MongoClientURI(config("uri")))
      mongoClient.getDatabase(config("database")).getCollection(collection).drop()
      mongoClient.close()
    }

    val mongoOutputFormat = MongoOutputFormat(config)
    dataset.output(mongoOutputFormat)
  }


}
