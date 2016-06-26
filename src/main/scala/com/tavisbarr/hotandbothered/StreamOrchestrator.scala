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
import Jama._ 

object StreamOrchestrator {

    implicit object MatrixAP extends AccumulatorParam[Matrix] {
     def zero(m: Matrix) = new Matrix(m.getRowDimension,m.getColumnDimension,0)
     def addInPlace(m1: Matrix , m2: Matrix) = {
       m1.plus(m2)
     }
    } 
  
  def main(args : Array[String] ) {    
      
    DatabaseWriter.createTableIfNecessary()
    val conf = new SparkConf().setMaster("local[3]").setAppName("HotAndBothered")
    val ssc = new StreamingContext(conf, Minutes(60))

    val xxAccum = ssc.sparkContext.accumulator(new Matrix(78,78,0))
    val xyAccum = ssc.sparkContext.accumulator(new Matrix(78,1,0))
    val repcounter = ssc.sparkContext.accumulator(0.0)
    val tempReducer = new Matrix(78,1,0)
    val hourReducer = new Matrix(78,1,0)
    val dowReducer = new Matrix(78,1,0)
    for (thisHour <- 0 to 23) {
      hourReducer.set(3*thisHour,0, thisHour)
      tempReducer.set(3*thisHour+1,0,1)
    }
    for ( thisDay <- 0 to 5 ) {
      dowReducer.set(thisDay+72,0,thisDay)
    }
    val consumerKey = Passwords.twitterConsumerKey
    val consumerSecret = Passwords.twitterConsumerSecret
    val accessToken = Passwords.twitterAccessToken
    val accessTokenSecret = Passwords.twitterAccessTokenSecret
    
    val twitter = new TwitterFactory().getInstance()
    twitter.setOAuthConsumer(consumerKey, consumerSecret)
    twitter.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret))
    
    //Start the streams
    val filters = Seq("fatal stabbing chicago","fatal shooting chicago")
    val twitterStream = TwitterUtils.createStream(ssc, Option(twitter.getAuthorization()), filters)
    val weatherStream = ssc.receiverStream(new WeatherReceiver())

    // Reduce the Twitter stream to a simple one-row RDD with the tweet count
    val twitterStreamCount = twitterStream.count()
    
    //Take the temperature, hour, and day of week elements from the last weather stream
    //and put the average of each into a one-row RDD
    val tempAndHourAndDOWAvg = weatherStream
          .map(v=>v.split(","))
          .map(v => (v(7).toDouble,v(3).toDouble,v(6).toDouble,1))
          .reduce((a ,b ) => (a._1+b._1,a._2+b._2,a._3+b._3,a._4+b._4))
          .map(v => Array(v._1/v._4,v._2/v._4,v._3/v._4))  
    
         
    val mytest = tempAndHourAndDOWAvg.map(v => (v(0).toDouble,v(1).toDouble,v(2).toDouble))
 
    weatherStream.print()
    val xx =  weatherStream
          .map(v => (v(7).toDouble,v(3).toDouble,v(6).toDouble,1))
          
    xx.print()
    mytest.print()
          
    //Turn into a temperature and temperature^2 variable for each hour of day, plus a day of week dummy
    val modelRHS = tempAndHourAndDOWAvg.map(v => {
      var hour = 0;
      var dow = 0;
      var hourlyTemps = Array(if (v(1) < 1) 1.0 else 0.0,if (v(1) < 1) v(0) else 0.0,if (v(1) < 1) v(0)*v(0) else 0.0)
      for ( hour <- 1 to 23 ) {
        hourlyTemps = hourlyTemps :+ (if (v(1) >= hour && v(1) < hour + 1) 1.0 else 0.0 ) 
        hourlyTemps = hourlyTemps :+ (if (v(1) >= hour && v(1) < hour + 1) v(0) else 0.0 ) 
        hourlyTemps = hourlyTemps :+ (if (v(1) >= hour && v(1) < hour + 1) v(0)*v(0) else 0.0 )
      }
      for ( dow <- 1 to 6 ) {
        hourlyTemps = hourlyTemps :+ (if (v(2) >= dow && v(2) < dow + 1 ) 1.0 else 0.0)
      }
      hourlyTemps
    })
    
    //Add a key of "1" to each (one-row) RDD so we can join them
    val modelRHSWithKey = modelRHS.map(v => (1,v))
    val twitterStreamCountWithKey = twitterStreamCount.map(v => (1,v))
    val dekeyedCombinedData = modelRHSWithKey.join(twitterStreamCountWithKey).map(a=>a._2) 

    //Observation goes into a LabeledPoint for estimation, right-hand side goes into a Vector for prediction
    val modelData = dekeyedCombinedData.map(v => LabeledPoint(v._2.toDouble,Vectors.dense(v._1)))
    val rhsData = dekeyedCombinedData.map(v => Vectors.dense(v._1))

    modelData.print()
    
   modelData.foreachRDD(r => {
      var b = if (repcounter.value >= 168) xxAccum.value.solve(xyAccum.value) else new Matrix(78,1,0)
      var thisYAndX  = r.collect()(0)
      var thisX = new Matrix(thisYAndX.features.toArray,1)
      var thisY = thisYAndX.label
      var thisxb = thisX.times(b).get(0,0)
      var thisXX = thisX.transpose().times(thisX)
      var thisXy = thisX.transpose().times(thisY)
      xxAccum += xxAccum.value.times(-0.01)
      xyAccum += xyAccum.value.times(-0.01)
      xxAccum += thisXX
      xyAccum += thisXy
      DatabaseWriter.addLine(thisxb , thisY.toLong , Array(thisX.times(tempReducer).get(0,0),thisX.times(hourReducer).get(0,0),thisX.times(dowReducer).get(0,0)))
      repcounter += 1
    })

      
    ssc.start()
    ssc.awaitTermination()

  }
 
 
}