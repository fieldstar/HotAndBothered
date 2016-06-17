package com.tavisbarr.hotandbothered

// Copyright 2016 Tavis Barr.  Licensed under Apache 2.0 license

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver

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
    
        try {
        while(!isStopped ) {
          val weatherJSON = currentWeather.fetchDataFromLocationToString("Chicago")
          currentWeather.addTimestamp()
          currentWeather.parseJSON(weatherJSON)
          val currentWeatherCSV = currentWeather.fetchTemparatureList()
          store(currentWeatherCSV)
          Thread.sleep(1200000)
        }
          
        } catch {
     case t: Throwable =>
       // restart if there is any other error
       restart("Error receiving data", t)
    }
    
  }
  
}