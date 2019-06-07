package top.lxpsee.statistics

import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/6/7 09:13.
  */

// 因为是离线统计分析，主要是针对评分
//"User-ID";"ISBN";"Book-Rating"
case class BookRating(userId: Int, isbn: Long, score: Double, timestamp: Long)

// MongoDB连接配置;url,要操作的db
case class MongoConfig(uri: String, db: String)

object StatisticsRecommender {
  // 定义mongoDB中的表名，统计分析结果表,热门商品，Recently 最近
  val MONGODB_RATING_COLLEXTION = "Rating"
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"
  val DATE_FORMAT_PATTERN = "yyyyMM"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://lanp:123@47.104.132.209:27017/recommender?authSource=admin",
      "mongo.db" -> "recommender"
    )

    val conf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingDF = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLEXTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[BookRating]
      .toDF()
    ratingDF.createOrReplaceTempView("ratings")

    // 历史热门商品，思路：按评分个数统计
    val rateMoreProductsSql = "SELECT isbn,COUNT(isbn) as cbid FROM ratings GROUP BY isbn ORDER BY cbid DESC"
    val rateMoreProductsDF = spark.sql(rateMoreProductsSql)
    storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    // 近期热门商品，需要把时间戳转换成yyyyMM格式进行评分个数统计:注册一个转换udf,
    val simpleDateFormat = new SimpleDateFormat(DATE_FORMAT_PATTERN)
    spark.udf.register("changDate", (x: Long) => simpleDateFormat.format(x))
    val ratingOfYearMonthSql = "SELECT isbn,score,changDate(timestamp) as yearmonth FROM ratings"
    val ratingOfYearMonthDF = spark.sql(ratingOfYearMonthSql)
    ratingOfYearMonthDF.createOrReplaceTempView("ratingsOfMonth")
    val rateMoreRecentlyProductsSql = "SELECT isbn,COUNT(isbn) as count,yearmonth FROM ratingsOfMonth GROUP BY yearmonth,isbn ORDER BY yearmonth DESC,count DESC"
    val rateMoreRecentlyProductsDF = spark.sql(rateMoreRecentlyProductsSql)
    storeDFInMongoDB(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

    // 优质商品：商品的平均评分
    val averageProductsSql = "SELECT isbn,AVG(score) as avg FROM ratings GROUP BY isbn ORDER BY avg DESC"
    val averageProductsDF = spark.sql(averageProductsSql)
    storeDFInMongoDB(averageProductsDF, AVERAGE_PRODUCTS)

    spark.stop()
  }

  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()
  }

}
