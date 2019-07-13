package com.config

import java.util.Properties

object ConfigManager {
  // 通过类加载器方法来加载指定的配置文件
  private val prop = new Properties()
  try{
    val jdbcConnect = ConfigManager.getClass.getClassLoader.getResourceAsStream("basic.properties")
    val dwd_dm = ConfigManager.getClass.getClassLoader.getResourceAsStream("dwd_dm.properties")
    val user_visit = ConfigManager.getClass.getClassLoader.getResourceAsStream("dm_user_visit.properties")
    prop.load(jdbcConnect)
    prop.load(dwd_dm)
    prop.load(user_visit)
  }catch {
    case e:Exception =>e.printStackTrace()
  }

  /**
    * 获取指定Key的对应Value
    */
  def getProper(key:String): String ={
    prop.getProperty(key)
  }
}
