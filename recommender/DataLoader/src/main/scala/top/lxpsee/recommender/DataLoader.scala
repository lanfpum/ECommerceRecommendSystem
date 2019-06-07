package top.lxpsee.recommender

import java.util.regex.{Matcher, Pattern}

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import top.lxpsee.recommender.utils.GenTimeStampUtils


/**
  * The world always makes way for the dreamer
  * Created by 努力常态化 on 2019/6/4 10:44.
  */

/**
  * Product 数据集,去判断是否需要参数，进而定义样例类
  * pid 0 ,商品名称 1，商品分类ID，亚马逊ID，商品的图片URL 4，商品分类 5，商品的UGC标签 6
  */
//case class Product(pid: Int, name: String, purl: String, categories: String, tags: String)

/**
  * Rating数据集,逗号分隔
  * 用户ID，商品ID，评分分数，时间戳
  *
  * 实操"User-ID";"ISBN";"Book-Rating"
  */
//case class Rating(uid: Int, pid: Int, score: Double, timestamp: Long)

// MongoDB连接配置;url,要操作的db
case class MongoConfig(uri: String, db: String)

//"ISBN";"Book-Title";"Book-Author";"Year-Of-Publication";"Publisher";"Image-URL-S";"Image-URL-M";"Image-URL-L"
case class Book(isbn: Long, title: String, author: String, yearOfPublication: String, publisher: String, images: String)

//"User-ID";"ISBN";"Book-Rating"
case class BookRating(userId: Int, isbn: Long, score: Double, timestamp: Long)

object DataLoader {
  // 定义文件位置常量，表名常量
  val BOOK_DATA_PATH = "D:\\IDEAWork\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\Book.csv"
  val BOOK_RATING_DATA_PATH = "D:\\IDEAWork\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\Book-Rating.csv"

  val MONGODB_BOOK_COLLECTION = "Book"
  val MONGODB_RATING_COLLEXTION = "Rating"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://lanp:123@47.104.132.209:27017/recommender?authSource=admin",
      "mongo.db" -> "recommender"
    )

    val conf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._
    val regex = ".*[a-zA-Z]+.*"

    val bookDF = spark.sparkContext
      .textFile(BOOK_DATA_PATH)
      .filter(_.split(";").length == 8)
      .filter(item => {
        val isbn = item.split(";")(0)
        val m = Pattern.compile(regex).matcher(isbn)
        !m.matches()
      })
      .map(item => {
        val books = item.split(";")
        Book(books(0).trim.toLong, books(1), books(2), books(3).trim, books(4), books(5))
      }).toDF()

    val ratingDF = spark.sparkContext.textFile(BOOK_RATING_DATA_PATH)
      .filter(_.split(";").length == 3)
      .filter(item => {
        val isbn = item.split(";")(1)
        val m = Pattern.compile(regex).matcher(isbn)
        !m.matches()
      })
      .map(item => {
        val ratings = item.split(";")
        BookRating(ratings(0).trim.toInt, ratings(1).trim.toLong, ratings(2).trim.toDouble,
          GenTimeStampUtils.getRandomDTimeStamp())
      }).toDF()

    // 多次地方都用到的mongoDB配置对象，可以封装成隐式转换对象，在使用的地方隐式传入就可以
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    storeDataInMongoDB(bookDF, ratingDF)
    spark.stop()
  }

  def storeDataInMongoDB(bookDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    // 创建一个mongodb的连接客户端，定义要操作的mongo表，可以理解为db.book,表如果存在则删除
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    val bookCollection = mongoClient(mongoConfig.db)(MONGODB_BOOK_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLEXTION)
    bookCollection.dropCollection()
    ratingCollection.dropCollection()

    // 将当前数据存入对应的表中，对表创建索引,评价表创建联合索引
    bookDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_BOOK_COLLECTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLEXTION)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()

    bookCollection.createIndex(MongoDBObject("isbn" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))
    ratingCollection.createIndex(MongoDBObject("isbn" -> 1))
  }

}
