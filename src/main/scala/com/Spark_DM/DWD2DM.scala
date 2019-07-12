package com.Spark_DM

import com.Constants.Contant
import com.SparkUtils.JDBCUtils
import com.config.ConfigManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object DWD2DM {
 // System.setProperty("hadoop.home.dir", "D:\\Java\\hadoop-2.8.1")
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName(Contant.SPARK_APP_NAME).master(Contant.SPARK_LOCAL).enableHiveSupport().getOrCreate()

    //获取sql语句
    val sql = ConfigManager.getProper(args(0))
    if(sql == null){
      LoggerFactory.getLogger("SparkLog").debug("表名参数有问题！请重新设置。。。")
    }else{
      val sqlResult:DataFrame= spark.sql(sql)
      //获取MySQL和hive的表名
      val mysqlTableName = args(0).split("[.]")(1)
      //存放到mysql
      val properties = JDBCUtils.getJDBCConnect()._1
      val url = JDBCUtils.getJDBCConnect()._2
      sqlResult.write.mode(SaveMode.Append).jdbc(url,mysqlTableName,properties)

      //存放到hive
      //sqlResult.write.mode(SaveMode.Overwrite).insertInto("qfbap_dm.dm_user_basic1")
    }
  }
}

