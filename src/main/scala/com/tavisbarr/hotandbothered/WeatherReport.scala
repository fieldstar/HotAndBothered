package com.tavisbarr.hotandbothered

// Copyright 2016 Tavis Barr.  Licensed under Apache 2.0 license


import scala.util.parsing.json._
import java.text.SimpleDateFormat
import java.util.Calendar

class WeatherReport {
  
  var year : Int = -1
  var month : Int = -1
  var dayOfMonth : Int = -1
  var hour : Int = -1
  var minute : Int = -1
  var second : Int = -1
  var dayOfWeek : Int = -1
  var location : String = null
  var tempCelsius : Double = -273.15
  var tempFarenheit : Double = -459.67
  var weatherDescription : String = null
  var lowTempCelsius : Double = -273.15
  var lowTempFarenheit : Double = -459.67
  var highTempCelsius : Double = -273.15
  var highTempFarenheit : Double = -459.67
  var windSpeed : Double = -1
  var latitude : Double = 361
  var longitude: Double = 361
 
  //Sample response:   
  //{"coord":{"lon":-87.65,"lat":41.85},"weather":[{"id":800,"main":"Clear","description":"clear sky","icon":"01d"}],"base":"cmc stations","main":{"temp":291.87,"pressure":1019,"humidity":44,"temp_min":290.37,"temp_max":295.26},"wind":{"speed":5.14,"deg":52,"gust":7.71},"clouds":{"all":0},"dt":1465745737,"sys":{"type":3,"id":41324,"message":0.1443,"country":"US","sunrise":1465726498,"sunset":1465781199},"id":4887398,"name":"Chicago","cod":200}
  
  def fetchDataFromLocationToString(locationString : String) : String = {
    location = locationString
    val url = "http://api.openweathermap.org/data/2.5/weather?q=" + location + "&APPID=" + Passwords.openWeatherAppID
    val result = scala.io.Source.fromURL(url).mkString
    //println(result)
    result
  }
  
  def addTimestamp() {
    val today = Calendar.getInstance.getTime
     
// create the date/time formatters
    val yearFormat = new SimpleDateFormat("yyyy")
    val monthFormat = new SimpleDateFormat("MM")
    val dayFormat = new SimpleDateFormat("dd")
    val hourFormat = new SimpleDateFormat("HH")
    val minuteFormat = new SimpleDateFormat("mm")
    val secondFormat = new SimpleDateFormat("ss")
    val dayOfWeekFormat = new SimpleDateFormat("EEE")
 
    year = yearFormat.format(today).toInt
    month = monthFormat.format(today).toInt
    dayOfMonth = dayFormat.format(today).toInt
    hour = hourFormat.format(today).toInt
    minute = minuteFormat.format(today).toInt
    second = secondFormat.format(today).toInt
    dayOfWeek = dayOfWeekFormat.format(today) match {
      case "Mon" => 1
      case "Tue" => 2
      case "Wed" => 3
      case "Thu" => 4
      case "Fri" => 5
      case "Sat" => 6
      case "Sun" => 7
    }
  }
  
  def parseJSON(rawJSON : String) {
    
    val jsonMap = JSON.parseFull(rawJSON)
    val tempKelvin = jsonMap.get.asInstanceOf[Map[String,Any]]("main").asInstanceOf[Map[String,Any]]("temp").asInstanceOf[Double]
    this.tempCelsius = tempKelvin - 273.15
    tempFarenheit = tempCelsius*1.8 + 32
    val lowTempKelvin = jsonMap.get.asInstanceOf[Map[String,Any]]("main").asInstanceOf[Map[String,Any]]("temp_min").asInstanceOf[Double]
    lowTempCelsius = lowTempKelvin - 273.15
    lowTempFarenheit = lowTempCelsius*1.8 + 32
    val highTempKelvin = jsonMap.get.asInstanceOf[Map[String,Any]]("main").asInstanceOf[Map[String,Any]]("temp_max").asInstanceOf[Double]
    highTempCelsius = highTempKelvin - 273.15
    highTempFarenheit = highTempCelsius*1.8 + 32
    weatherDescription = jsonMap.get.asInstanceOf[Map[String,Any]]("weather").asInstanceOf[List[Map[String,Any]]](0).asInstanceOf[Map[String,Any]]("description").asInstanceOf[String]
    windSpeed = jsonMap.get.asInstanceOf[Map[String,Any]]("wind").asInstanceOf[Map[String,Any]]("speed").asInstanceOf[Double]
    latitude = jsonMap.get.asInstanceOf[Map[String,Any]]("coord").asInstanceOf[Map[String,Any]]("lat").asInstanceOf[Double]
    longitude = jsonMap.get.asInstanceOf[Map[String,Any]]("coord").asInstanceOf[Map[String,Any]]("lon").asInstanceOf[Double]
    
    /*println("Result: location: " + location + ", tempCelsuis: " + tempCelsius 
        + ", tempFarenheit: " + tempFarenheit + ", weatherDescription: " + weatherDescription + ", lowTempCelsius: "
        + lowTempCelsius + ", lowTempFarenheit: " + lowTempFarenheit + ", highTempCelsius: " + highTempCelsius
        + "highTempFarenheit: " + highTempFarenheit + ", windSpeed: " + windSpeed + ", latitude: " + latitude 
        + ", longitude: " + longitude)*/
    
    
    
  }
  
  def fetchTemparatureList(): String = {
    "" + year + "," + month + "," + dayOfMonth + "," + hour + "," + minute + "," + second + "," + dayOfWeek +
    "," + tempFarenheit + "," + lowTempFarenheit + "," + highTempFarenheit + "," + latitude + "," + longitude
  }
  
}