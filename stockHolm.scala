package com.sparktest.spark.stockhome

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.log4j.Logger

object stockHolm {

  def main(args:Array[String]):Unit={

    val spark:SparkSession = SparkSession.builder()
      .master("local[1]").appName("stockhometcs.com")
      .getOrCreate();
     	
    import spark.implicits._
    val log = Logger.getLogger(this.getClass.getName)
    
    log.info("Loading Raw individual temperature observations ")
    
    val myUDf = udf((s:String) => Array(s.trim.replaceAll(" +", " ")))
    
    val rawTemp2017df = spark.read.format("text")
                   .option("header","false")
                   .load("/resources/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt","/resources/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")
                   .toDF("val")
    val rawTemp1858df = spark.read.format("text")
                   .option("header","false")
                   .load("/resources/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
                   .toDF("val")
	  
    val rawTemp1960df = spark.read.format("text")
                   .option("header","false")
                   .load("/resources/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
                   .toDF("val")
                   
    log.info("Formatting for data between 1961 to 2017")
    val rawTempFrmt2017df = rawTempdf
                .withColumn("udfResult",myUDf(col("val")))
                .withColumn("new_val", col("udfResult")(0))
                .select(split(col("new_val")," ").getItem(0).as("year"),split(col("new_val")," ").getItem(1).as("month"),
                        split(col("new_val")," ").getItem(2).as("date"),split(col("new_val")," ").getItem(3).as("morning_temp"),
                        split(col("new_val")," ").getItem(4).as("noon_temp"),split(col("new_val")," ").getItem(5).as("evening_temp"),
                        split(col("new_val")," ").getItem(6).as("temp_min"),split(col("new_val")," ").getItem(7).as("temp_max"),
                        split(col("new_val")," ").getItem(8).as("temp_mean"))
            
                  
    log.info("Formatting for data between 1756 to 1858")              
    val rawTempFrmt1858df = rawTempdf
                .withColumn("udfResult",myUDf(col("val")))
                .withColumn("new_val", col("udfResult")(0))
                .select(split(col("new_val"),",").getItem(0).as("year"),split(col("new_val"),",").getItem(1).as("month"),
                        split(col("new_val"),",").getItem(2).as("date"),split(col("new_val"),",").getItem(3).as("morning_temp"),
                        split(col("new_val"),",").getItem(4).as("noon_temp"),split(col("new_val"),",").getItem(5).as("evening_temp"),
                        lit(null).as("temp_min"),lit(null).as("temp_max"),lit(null).as("temp_mean"))
                
                

    log.info("Formatting for data between 1859 to 1960")               
    val rawTempFrmt1960df = rawTempdf
                .withColumn("udfResult",myUDf(col("val")))
                .withColumn("new_val", col("udfResult")(0))
                .select(split(col("new_val"),",").getItem(0).as("year"),split(col("new_val"),",").getItem(1).as("month"),
                        split(col("new_val"),",").getItem(2).as("date"),split(col("new_val"),",").getItem(3).as("morning_temp"),
                        split(col("new_val"),",").getItem(4).as("noon_temp"),split(col("new_val"),",").getItem(5).as("evening_temp"),
                        split(col("new_val"),",").getItem(6).as("temp_min"),split(col("new_val"),",").getItem(7).as("temp_max"),
                        lit(null).as("temp_mean"))
                .drop("new_val")
                
    log.info("Merging all Tempreture data") 
    
    val dailyTempDf = rawTempFrmt1960df.unionByName(rawTempFrmt1858df).unionByName(rawTempFrmt2017df)
                   
    log.info("Loading Barometer Reading ")
    
    val rawPressure2017df = spark.read.format("text").option("header","false")
                   .load("/resources/stockholm_barometer_1938_1960.txt",
                         "/resources/stockholm_barometer_1961_2012.txt","/resources/stockholm_barometer_2013_2017.txt")
                   .toDF("val")
	  
    val rawPressure1937df = spark.read.format("text").option("header","false")
                   .load("/resources/stockholm_barometer_1862_1937.txt")
                   .toDF("val")
	  
    val rawPressure1861df = spark.read.format("text").option("header","false")
                   .load("/resources/stockholm_barometer_1859_1861.txt")
                   .toDF("val")
    val rawPressure1858df = spark.read.format("text").option("header","false")
                   .load("/resources/stockholm_barometer_1756_1858.txt")
                   .toDF("val")
                   
    log.info("Formatting for data between 1938 to 2017")
    val rawPressureFrmt2017df = rawPressure2017df
                .withColumn("udfResult",myUDf(col("val")))
                .withColumn("new_val", col("udfResult")(0))
                .select(split(col("new_val")," ").getItem(0).as("year"),split(col("new_val")," ").getItem(1).as("month"),
                        split(col("new_val")," ").getItem(2).as("date"),0.75*(split(col("new_val")," ").getItem(3)).as("morning_pressure"),
                        0.75*(split(col("new_val")," ").getItem(4)).as("noon_pressure"),0.75*(split(col("new_val")," ").getItem(5)).as("evening_pressure"))
        
    log.info("Formatting for data between 1859 to 1861")
    val rawPressureFrmt1937df = rawPressure1937df
                .withColumn("udfResult",myUDf(col("val")))
                .withColumn("new_val", col("udfResult")(0))
                .select(split(col("new_val")," ").getItem(0).as("year"),split(col("new_val")," ").getItem(1).as("month"),
                        split(col("new_val")," ").getItem(2).as("date"),split(col("new_val")," ").getItem(3).as("morning_pressure"),
                        split(col("new_val")," ").getItem(4).as("noon_pressure"),split(col("new_val")," ").getItem(5).as("evening_pressure"))
                .drop("new_val","val")
            
    log.info("Formatting for data between 1859 to 1861")
    val rawPressureFrmt1861df = rawPressure1861df
                .withColumn("udfResult",myUDf(col("val")))
                .withColumn("new_val", col("udfResult")(0))
                .select(split(col("new_val")," ").getItem(0).as("year"),split(col("new_val")," ").getItem(1).as("month"),
                        split(col("new_val")," ").getItem(2).as("date"),split(col("new_val")," ").getItem(3).as("morning_pressure"),
                        split(col("new_val")," ").getItem(6).as("noon_pressure"),split(col("new_val")," ").getItem(9).as("evening_pressure"))
                .drop("new_val","val")
                
    log.info("Formatting for data between 1756 to 1858")           
    val rawPressureFrmt1858df = rawPressure1858df
                .withColumn("udfResult",myUDf(col("val")))
                .withColumn("new_val", col("udfResult")(0))
                .select(split(col("new_val")," ").getItem(0).as("year"),split(col("new_val")," ").getItem(1).as("month"),
                        split(col("new_val")," ").getItem(2).as("date"),split(col("new_val")," ").getItem(3).as("morning_pressure"),
                        split(col("new_val")," ").getItem(5).as("noon_pressure"),split(col("new_val")," ").getItem(7).as("evening_pressure"))
                .drop("new_val","val")
                
    val baroMeterDf = rawPressureFrmt1858df.unionByName(rawPressureFrmt1861df).unionByName(rawPressureFrmt2017df).unionByName(rawPressureFrmt1937df)
        
        
    val partionkey= "year"
    val fileformat= "orc"
	  
    log.info("Loading data into HDFS Paths")
    dailyTempDf.write().mode(SaveMode.Overwrite).partitionBy(partionkey).format(fileformat).save("hdfs://namenodeaddress:8020/usr/hive/warehouse/<appname/dbname>/<dailytemptable>/")
    baroMeterDf.write().mode(SaveMode.Overwrite).partitionBy(partionkey).format(fileformat).save("hdfs://namenodeaddress:8020/usr/hive/warehouse/<appname/dbname>/<barometertable>/")
  }
}
    
