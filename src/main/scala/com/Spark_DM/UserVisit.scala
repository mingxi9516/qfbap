package com.Spark_DM

import com.Constants.Contant
import com.SparkUtils.JDBCUtils
import com.config.ConfigManager
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object UserVisit {
  def main(args: Array[String]): Unit = {
    //创建sparksession
    val spark = SparkSession.builder().appName(Contant.SPARK_APP_NAME).master(Contant.SPARK_LOCAL).enableHiveSupport().config("spark.debug.maxToStringFields", "100").getOrCreate()

    //获取综合查询sql语句
    val pc_app_sql = ConfigManager.getProper("qfbap_dm.dm_user_visit")
    //判断
    if(pc_app_sql == null){
      LoggerFactory.getLogger("SparkLog").debug("获取sql语句错误")
    }else{
      //综合查询
      val result = spark.sql(pc_app_sql)
      result.show()
      //存入MySQL
      val prop = JDBCUtils.getJDBCConnect()._1
      val url = JDBCUtils.getJDBCConnect()._2
      result.write.mode(SaveMode.Append).jdbc(url,"dm_user_visit",prop)

      //存入hive
      //result.write.mode(SaveMode.Overwrite).saveAsTable("qfbap_dm.dm_user_visit")
    }


  }
}
