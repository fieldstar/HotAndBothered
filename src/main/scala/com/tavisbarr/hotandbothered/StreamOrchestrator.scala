package com.tavisbarr.hotandbothered

// Copyright 2016 Tavis Barr.  Licensed under Apache 2.0 license

import org.apache.spark._
import org.apache.spark.mllib.regression._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import twitter4j.TwitterFactory
import twitter4j.auth.AccessToken
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
 

object StreamOrchestrator {
  
  def main(args : Array[String] ) {    
      
    DatabaseWriter.createTableIfNecessary()
    val conf = new SparkConf().setMaster("local[3]").setAppName("HotAndBothered")
    val ssc = new StreamingContext(conf, Minutes(60))
    val counter = ssc.sparkContext.accumulator(0L, "Counter")

    val consumerKey = "GOCwh678jYFkdzhwCsnm0k5cw"
    val consumerSecret = "mS4m7yvPkZbBe2G7oV5sYB62wVz5qbuEaThyZyvuJtvPcUjOYb"
    val accessToken = "742529982296100865-ViWtDejUzeqYHu4NInxAiRyaIR08Vjp"
    val accessTokenSecret = "RLEx8LBLokp7tVEqmG0wA1CO1UotAMgE3S6vcSpLMZd6w"
    
    val twitter = new TwitterFactory().getInstance()
    twitter.setOAuthConsumer(consumerKey, consumerSecret)
    twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret))
    
    val numFeatures = 54
    val model = new StreamingLinearRegressionWithSGD()
      .setInitialWeights(Vectors.zeros(numFeatures))
      .setNumIterations(30)
      .setMiniBatchFraction(1.0)
      .setStepSize(0.5)
    
    //Get the Twitter stream
    val filters = Seq("fatal stabbing chicago","fatal shooting chicago")
    val twitterStream = TwitterUtils.createStream(ssc, Option(twitter.getAuthorization()), filters)
    val weatherStream = ssc.receiverStream(new WeatherReceiver())

    // Reduce it to a simple one-row RDD with the tweet count
    val twitterStreamCount = twitterStream.count()
    
    //Take the temperature, hour, and day of week elements from the last weather stream
    //and put the average of each into a one-row RDD
    val tempAndHourAndDOW = weatherStream.map(_.split(",")).map(v => Array(v(7).toDouble,v(3).toDouble,v(6).toDouble,1))
    val tempAndHourAndDOWObs = tempAndHourAndDOW.count()
    val tempAndHourAndDOWSum = tempAndHourAndDOW.reduce((a ,b ) => Array(a(0)+b(0),a(1)+b(1),a(2)+b(2),a(3)+b(3)))
    val tempAndHourAndDOWAvg = tempAndHourAndDOWSum.map(v => Array(v(0)/v(3),v(1)/v(3),v(2)/v(3)))

    //Turn into a temperature and temperature^2 variable for each hour of day, plus a day of week dummy
    val modelRHS = tempAndHourAndDOWAvg.map(v => {
      var hour = 0;
      var dow = 0;
      var hourlyTemps = Array(if (v(1) < 1) v(2) else 0,if (v(1) < 1) v(2)*v(2) else 0)
      for ( hour <- 1 to 23 ) {
        hourlyTemps = hourlyTemps :+ (if (v(1) >= hour && v(1) < hour + 1) v(2) else 0 )
        hourlyTemps = hourlyTemps :+ (if (v(1) >= hour && v(1) < hour + 1) v(2)*v(2) else 0 )
      }
      for ( dow <- 1 to 6 ) {
        hourlyTemps = hourlyTemps :+ (if (v(2) >= dow && dow < dow + 1 ) 1.0 else 0.0)
      }
      hourlyTemps
    })
    
    //Add a key of "1" to each (one-row) RDD so we can join them
    val modelRHSWithKey = modelRHS.map(v => (1,v))
    val twitterStreamCountWithKey = twitterStreamCount.map(v => (1,v))
    val combinedData = modelRHSWithKey.join(twitterStreamCountWithKey)
    val dekeyedCombinedData = combinedData.map(a=>a._2) 

    //Observation goes into a LabeledPoint for estimation, right-hand side goes into a Vector for prediction
    val modelData = dekeyedCombinedData.map(v => LabeledPoint(v._2.toDouble,Vectors.dense(v._1)))
    val rhsData = dekeyedCombinedData.map(v => Vectors.dense(v._1))

    //Use previous iteration's model to predict this iteration's count
    val prediction = model.predictOn(rhsData)
    val predictionWithKey = prediction.map(v => (1,v))
    val predictionWithDataWithKey = predictionWithKey.join(combinedData)
    val predictionWithData = predictionWithDataWithKey.map(v => v._2)
    

    //Write results to the dATABASE
    predictionWithData.foreachRDD(r => {
      val (pred , (weather , count ) ) = r.collect()(0)
      DatabaseWriter.addLine(pred , count , weather)
    })
    predictionWithData.print()

    //Train on the new data for next interval's prediction
    model.trainOn(modelData)
      
    ssc.start()
    ssc.awaitTermination()

  }
 
 
}