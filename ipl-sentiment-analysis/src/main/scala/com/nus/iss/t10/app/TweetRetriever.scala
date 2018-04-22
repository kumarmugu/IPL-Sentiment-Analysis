package com.nus.iss.t10.app

import java.io.Serializable

import com.typesafe.config.ConfigFactory
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status



object TweetRetriever extends Serializable {

	def main(args: Array[String]) : Unit = {
		if (args.length < 1){
			println("Please specify the search string as argument")
			System.exit(-1)
		}
		val searchString = args mkString " "

		// Load properties for config file
		val config = ConfigFactory.load().getConfig("app.env")

		val fileName = config.getString("fileLocation")
		println(s"My secret value is $fileName")

		val host = config.getString("sparkHost")

		val sparkConf = new SparkConf().setAppName("Ipl Sentiment Analysis").setMaster(host)
		//val sparkConf = new SparkConf().setAppName("Twitter Streaming")
    val sparkContext = new SparkContext(sparkConf)

		val sc = new StreamingContext(sparkContext, Seconds(5))
    val sqlContext= new SQLContext(sparkContext)
    import sqlContext.implicits._


    // twitter properties
		val consumerKey = config.getString("Twitter.secret.consumerKey")
		val consumerSecret = config.getString("Twitter.secret.consumerSecret")
		val accessToken = config.getString("Twitter.secret.accessToken")
		val accessTokenSecret = config.getString("Twitter.secret.accessTokenSecret")

		System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
		System.setProperty("twitter4j.oauth.accessToken", accessToken)
		System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)


		val inputDStream : ReceiverInputDStream[Status] = TwitterUtils.createStream(sc, None, Array(searchString))

		inputDStream.foreachRDD((rdd : RDD[twitter4j.Status]) => rdd.foreach(s => println(s.getText)))


		inputDStream.foreachRDD((rdd : RDD[twitter4j.Status]) => {
			def mapTweet(status: twitter4j.Status) : Tweet = {
				val user = status.getUser
				val place = status.getPlace
				Tweet(status.getId,
					if (user == null) 0L else user.getId,
					if (user == null) "" else user.getName,
					if (user == null) "" else user.getScreenName,
					if (user == null) 0 else user.getFollowersCount,
					status.getText,
					status.getLang,
					status.isRetweeted,
					status.isRetweetedByMe,
					status.getSource,
					status.getFavoriteCount,
					if (place == null) "" else place.getCountry
				)
			}

			def isEnglishTweet(t: Tweet) = t.lang == "en"


			rdd.map(mapTweet).filter(isEnglishTweet).map(_.toString).toDF.write.mode("append").save(("hdfs://localhost:8020/user/cloudera/workspace/tweets"))
		})

		sc.start
    sc.awaitTermination

	}

  case class Tweet(id: Long, userId: Long, user: String, screenName: String, followerCount: Int, text: String, lang: String, isRt : Boolean, isSelfRt: Boolean, source: String, favCount: Int, country: String) extends java.io.Serializable {
    override def toString = id.toString + "\t" + userId.toString + "\t" + user + "\t" + screenName + "\t" + followerCount + "\t" + text + "\t" + lang + "\t" + isRt.toString + "\t" + isSelfRt.toString + "\t" + source + "\t" + favCount.toString + "\t" + country
  }


}