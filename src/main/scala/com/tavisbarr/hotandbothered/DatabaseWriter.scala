package com.tavisbarr.hotandbothered

// Copyright 2016 Tavis Barr.  Licensed under Apache 2.0 license

import java.sql.DriverManager
import java.sql.Connection

object DatabaseWriter {
  
  def addLine(pred : Double , count: Long , weather : Array[Double] ) : Unit = {
 
    val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost/weather?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=EST"
    //
    val username = "weatheruser"
    val password = "weatherpassword"

    var con:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      con = DriverManager.getConnection(url, username, password)
      } catch {
      case e : Throwable => {
        println("Error opening SQL connection")
        e.printStackTrace
        con.close()
      }
    }
    println("Writing " + count + ", " + weather(0) + ", " + weather(1) + ", " + pred)
    val query : String = "INSERT INTO record ( count , tempf , hour , prediction ) VALUES( ? , ? , ? , ?  )"
    try {
      val st = con.prepareStatement(query)
      st.setLong(1, count)
      st.setDouble(2, weather(0))
      st.setDouble(3, weather(1))
      if ( !pred.isNaN() && !pred.isInfinite() ) {
      st.setDouble(4, pred)        
      }
      else {
        st.setObject(4,null)
      }
      st.executeUpdate()
      con.close()
     } catch {
      case e : Throwable => {
        println("Error performing SQL update")
        e.printStackTrace
  
      }
    }
      
  }
  
  def createTableIfNecessary() {

        val driver = "com.mysql.cj.jdbc.Driver"
    val url = "jdbc:mysql://localhost/weather?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=EST"
    //
    val username = "weatheruser"
    val password = "weatherpassword"
    var tableExists = false
    
    var con:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      con = DriverManager.getConnection(url, username, password)
      } catch {
      case e : Throwable => {
        println("Error opening SQL connection")
        e.printStackTrace
        con.close()
      }
    }

    try {
      val st = con.prepareStatement("SHOW TABLES LIKE 'record'")
      val resultSet = st.executeQuery()
      
      if (resultSet.next()) {
        tableExists = true
      }
      if (resultSet != null ) {
        resultSet.close()
      }
      
      if ( !tableExists ) {
        val st2 = con.prepareStatement("CREATE TABLE record (" +
	        " count INT(10) , " + 
	        " highf DOUBLE , " + 
	        " lowf DOUBLE , " + 
	        " hour DOUBLE , " + 
	        " prediction DOUBLE , " + 
	        " updatetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)")
	        st2.executeUpdate()
	        if ( st2 != null ) {
	          st2.close()
	        }
      }
 
        
      con.close()
     } catch {
      case e : Throwable => {
        println("Error performing SQL update")
        e.printStackTrace
  
      }
    }

  }

}