package com.nus.iss.t10.app

import java.io.Serializable

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming._
import org.apache.spark.streaming.api.java._
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status
import twitter4j.conf._
import twitter4j.auth.OAuthAuthorization
import com.nus.iss.t10.utils.SentimentAnalysisUtils._

/**
	*  Team - 10
	*/
object StreamingApp extends Serializable {

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

		val sc = new StreamingContext(sparkConf, Seconds(5))

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
		

		inputDStream.foreachRDD(processTweetRDD(_))

		sc.start
		sc.awaitTermination
	}

	def processTweetRDD(rdd: RDD[twitter4j.Status]) : Unit ={
		//do cl
		val results : scala.collection.Map[String, Long] = rdd.filter(_.getLang == "en").map(_.getText).map((txt: String) => {
			detectSentiment(txt).toString
		}).countByValue
		//reduce resultRDD on count by classification

		// //sample data
		// import scala.util.Random
		// val rand = new Random
		// val results = Array(("POSITIVE", rand.nextInt(30)), 
		// 	("NEGATIVE",rand.nextInt(30)), 
		// 	("NEUTRAL", rand.nextInt(20))).map(a => {
		// 		a._1 + ":" + a._2.toString}).mkString("|")

		val message = results.toArray.map((a:(String, Long)) => {
		 		a._1 + ":" + a._2.toString}).mkString("|")
		//send to message broker
		redis.RedisProducer.publishResults(message)
	}
}