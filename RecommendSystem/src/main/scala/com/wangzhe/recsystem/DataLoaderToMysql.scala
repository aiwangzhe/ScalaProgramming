package com.wangzhe.recsystem

class DataLoaderToMysql {

  def main(args: Array[String]): Unit = {
    // movieWithTagsData.writeAsText("output")



    //    // get a TableEnvironment
    //    val tableEnv = TableEnvironment.getTableEnvironment(environment)
    //
    //    tableEnv.registerDataSet("movieTags", movieWithTagsData)
    //
    ////    val table = tableEnv.sqlQuery("select * from movieTags")
    ////    val dataset = tableEnv.toDataSet[(Int, String, String)](table)
    ////    dataset.print()
    //
    //    val jdbcSink = JDBCAppendTableSink.builder().setDrivername("com.mysql.jdbc.Driver")
    //        .setDBUrl("jdbc:mysql://node1:3306/recommender?characterEncoding=utf8&useSSL=false")
    //        .setUsername("wangzhe")
    //        .setPassword("Wang112233")
    //        .setParameterTypes(Types.INT, Types.STRING, Types.STRING)
    //        .setQuery("insert into movieTags(mid, name ,tag) values (?, ?, ?)")
    //        .build()
    //
    //    tableEnv.registerTableSink(
    //      "movieTagsTable",
    //      Array("mid", "name", "tag"),
    //      Array(Types.INT, Types.STRING, Types.STRING),
    //      jdbcSink);
    //
    //    val table = tableEnv.sqlQuery("select * from movieTags")
    //    table.insertInto("movieTagsTable");

    // execute the program
  }

}
