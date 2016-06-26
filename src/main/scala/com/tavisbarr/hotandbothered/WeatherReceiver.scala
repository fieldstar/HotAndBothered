package com.tavisbarr.hotandbothered

// Copyright 2016 Tavis Barr.  Licensed under Apache 2.0 license

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver
import java.util.Date

class WeatherReceiver extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2)  {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
   // There is nothing much to do as the thread calling receive()
   // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {

        val currentWeather  = new WeatherReport()
    
        var lastCheckTime = new Date().getTime
        try {
        while(!isStopped ) {
          var currentTime = new Date().getTime
            if ( currentTime - lastCheckTime > 120000 ) {
            var weatherJSON : String = null;
            try {
              weatherJSON = currentWeather.fetchDataFromLocationToString("Chicago")
            } catch {
             case t: Throwable =>
               println("Error receiving data at " + new Date() + ": " + t.getMessage)
            }
            if ( weatherJSON != null ) {
              currentWeather.addTimestamp()
              currentWeather.parseJSON(weatherJSON)
              //val currentWeatherCSV = currentWeather.fetchTemparatureList()
              store(currentWeather.fetchTemparatureList())              
              println("Weather checked at " + new Date() + ": " + weatherJSON)
            }
            lastCheckTime = currentTime
          }
        }
          
        } catch {
     case t: Throwable =>
       // restart if there is any other error
       restart("Error receiving data", t)
    }
    
  }
  
}