package com.streamapp.kafka

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, SparkSession}

object milkBasketML {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Spark-Kafka")

    val spark = SparkSession.builder.config(conf).master("local").appName("KafkaSparkStreaming").getOrCreate()
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    val customerDF : DataFrame = spark.read.option("inferSchema", true).option("header",true).csv("file:///root/temp1/hackathon_data.csv")

    customerDF.createOrReplaceTempView("customerData")

    val newdf : DataFrame = spark.sql("select distinct(customer_id) cus_id, subcategory_id ,subscription from customerData")


    //var newdf = spark.read.parquet("file:///root/temp1/data/file/")

    val Array(training, test) = newdf.randomSplit(Array(0.8, 0.2))

    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("cus_id")
      .setItemCol("subcategory_id")
      .setRatingCol("subscription")

    val model = als.fit(training)

    model.setColdStartStrategy("drop")

    val predictions = model.transform(test)


    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("subscription")
      .setPredictionCol("prediction")

    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")

    val userRecs = model.recommendForAllUsers(10)

    userRecs.show(false)

    val productRecs = model.recommendForAllItems(10)

    productRecs.show(false)




  }

}
