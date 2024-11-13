package org.apache.spark.sql.lakesoul.entry

import org.apache.spark.sql.SparkSession

import scala.sys.exit

object SqlSubmitter {
  def main(args: Array[String]): Unit = {
    val usage = """
        Usage: spark-submit --class org.apache.spark.sql.lakesoul.entry.SqlSubmitter lakesoul-spark.jar --sql-file s3/hdfs --schedule-time=timestamp_in_milliseconds
      """

    if (args.length == 0) {
      println(usage)
      exit(1)
    }

    def nextArg(map: Map[String, Any], list: List[String]): Map[String, Any] = {
      list match {
        case Nil => map
        case "--sql-file" :: value :: tail =>
          nextArg(map ++ Map("sql-file" -> value), tail)
        case "--schedule-time" :: value :: tail =>
          nextArg(map ++ Map("schedule-time" -> value), tail)
        case unknown :: _ =>
          println("Unknown option " + unknown)
          exit(1)
      }
    }
    val options = nextArg(Map(), args.toList)
    if (!options.contains("sql-file") || !options.contains("schedule-time")) {
      println(usage)
      exit(1)
    }
    println(options)

    val spark = SparkSession.builder().getOrCreate()

    val sqlContent = spark.sparkContext.wholeTextFiles(options("sql-file").toString).take(1)(0)._2
    println(sqlContent)

    val sqlStatement = sqlContent.split(";")
    sqlStatement.foreach(sql => {
      val sqlReplaced = sql.replaceAll("\\$\\{scheduleTime}", options("schedule-time").toString)
      println(s"==========Executing: $sqlReplaced")
      println("====================================================================")
      spark.sql(sqlReplaced).show()
    })
  }
}
