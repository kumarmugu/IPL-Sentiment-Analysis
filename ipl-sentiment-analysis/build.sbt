name := "IPL Sentiment Analysis"
version := "1.0"
//scalaVersion := "2.11.8"

scalaVersion := "2.10.5"

//val sparkVersion = "2.2.0"
val sparkVersion = "1.5.2"

resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.3",
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "redis.clients" % "jedis" % "2.9.0",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2",
  "edu.stanford.nlp" % "stanford-corenlp" % "3.5.2" classifier "models",
  "com.typesafe" % "config" % "1.2.0"


)