package com.test.kudu

import java.text.SimpleDateFormat
import java.util.Calendar

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

case class BaseStatKey(guid: String, app: String, plat: String, version: String, channel: String)
case class BaseStatResultKey(app: String, plat: String, version: String, channel: String)
case class TotalBaseLogin(timestamp: Long, guid: String, app: String, plat: String, version: String, channel: String, var reason: String) {
  override def toString(): String = {
    s"${this.timestamp}|${this.guid}|${this.app}|${this.plat}|${this.version}|${this.channel}|${this.reason}"
  }
}
object LogUtils {
  def main(args: Array[String]) {
    val xua = """SN=IOS_com.upchina.bhtrader&VN=4.3.3.4_4.3.3&MO=iPhone&VC=Apple&RL=750_1334&RV=&OS=10.2&CHID=11_&MN=taf"""
    println(parseXuaALL(xua))
  }

  /**
   * 解析XUA <br>
   * reutrn   (app,plat,os,version,channel)
   * @param xua
   * @return  app,plat,os,version,channel
   */
  def parseXUA(xua: String): (String, String, String, String, String, String) = {
    val keyAndValues = xua.split("&")
      .map(x =>
        {
          val splits = x.split("=")
          if (splits.size == 2) {
            (splits(0), splits(1))
          } else {
            (splits(0), splits(0))
          }
        }).toMap
    val app = keyAndValues("MN")
    val (plat, pkgName) = { val sn = keyAndValues("SN"); val index = sn.indexOf("_"); (sn.substring(0, index), sn.substring(index + 1)) }
    val isPC = "WIN".equalsIgnoreCase(plat)
    val os = keyAndValues("OS")
    val version = { val vns = keyAndValues("VN").split("_"); if (isPC) vns(0) else vns(1) }
    val channel = (keyAndValues("CHID").split("_",-1))(0)
    (app, plat, os, version, channel, pkgName)
  }

  /**
   * 解析XUA <br>
   * reutrn   (app,plat,os,version,channel,mo, vc, rl_width,  rl_height)
   * @param xua
   * @return  app,plat,os,version,channel
   */
  def parseXuaALL(xua: String): Option[(String, String, String, String, String, String, String, String, String, String)] = {
    val keyAndValues = xua.split("&")
      .map(x =>
        {
          val splits = x.split("=")
          if (splits.size == 2) {
            (splits(0), splits(1))
          } else {
            (splits(0), "")
          }
        }).toMap
    val app = keyAndValues("MN")
    val (plat, pkgName) = { val sn = keyAndValues("SN"); val index = sn.indexOf("_"); (sn.substring(0, index), sn.substring(index + 1)) }
    val isPC = "WIN".equalsIgnoreCase(plat)
    val os = keyAndValues("OS")
    val version = { val vns = keyAndValues("VN").split("_"); if (isPC) vns(0) else vns(1) }
    val channel = //if (plat.equalsIgnoreCase("IOS")) {
      // Option("9001")
      //} else {
      {
        val chids = keyAndValues("CHID").split("_",-1)
        if (!chids(0).equals("")) {
          Option(chids(0))
        } else if (chids.length > 1) {
          if (!chids(1).equals("")) {
            Option(chids(1))
          } else {
            Option[String](null)
          }
        } else {
          Option[String](null)
        }
      }
    if (channel.isEmpty) {
      Option[(String, String, String, String, String, String, String, String, String, String)](null)
    } else {
      val mo = keyAndValues.getOrElse("MO", { "" })
      val vc = keyAndValues.getOrElse("VC", "")
      val rl = keyAndValues.getOrElse("RL", "0_0")
      val Array(width, height) = rl.split("_")
      Option((app, plat, os, version, channel.get, mo, vc, width, height, pkgName))
    }
  }

  def parseTimestamp(ts: String): Long = {
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    format.parse(ts).getTime
  }

  def getBeforeDay(dateStr: String, beforeDay: Int): String = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance;
    val date = dateFormat.parse(dateStr)
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_YEAR, -beforeDay)
    val tmpDateStr = dateFormat.format(calendar.getTime)
    tmpDateStr
  }
  def getBeforeTimeStamp(dateStr: String, beforeDay: Int): Long = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance;
    val date = dateFormat.parse(dateStr)
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_YEAR, -beforeDay)
    calendar.getTime.getTime

  }
  def getNextDay(dateStr: String, nextDay: Int): String = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance;
    val date = dateFormat.parse(dateStr)
    calendar.setTime(date)
    calendar.add(Calendar.DAY_OF_YEAR, nextDay)
    val tmpDateStr = dateFormat.format(calendar.getTime)
    tmpDateStr
  }
  def getBeforeMuttDays(dateStr: String, beforeDay: Int): Array[String] = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance;
    val date = dateFormat.parse(dateStr)
    calendar.setTime(date)
    val result = new Array[String](beforeDay)
    1.to(beforeDay).foreach { x =>
      calendar.add(Calendar.DAY_OF_YEAR, -1)
      val tmpDateStr = dateFormat.format(calendar.getTime)
      result(x - 1) = tmpDateStr
    }
    result
  }

  def getNextMuttDays(dateStr: String, nextDay: Int): Array[String] = {
    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    val calendar = Calendar.getInstance;
    val date = dateFormat.parse(dateStr)
    calendar.setTime(date)
    val result = new Array[String](nextDay)
    1.to(nextDay).foreach { x =>
      calendar.add(Calendar.DAY_OF_YEAR, 1)
      val tmpDateStr = dateFormat.format(calendar.getTime)
      result(x - 1) = tmpDateStr
    }
    result
  }

  def newLogin = 1
  def activeLoginReason = "active";
  def allKey = "-1"
  def flatMap(baseStatKey: BaseStatKey): Seq[(BaseStatKey, BaseStatKey)] = {
    var list = scala.collection.mutable.Seq[(BaseStatKey, BaseStatKey)]()
    val baseStatKey1 = BaseStatKey(baseStatKey.guid, baseStatKey.app, allKey, allKey, allKey)
    list = list.:+((baseStatKey1, baseStatKey))
    val baseStatKey2 = BaseStatKey(baseStatKey.guid, baseStatKey.app, allKey, allKey, baseStatKey.channel)
    list = list.:+((baseStatKey2, baseStatKey))
    val baseStatKey3 = BaseStatKey(baseStatKey.guid, baseStatKey.app, allKey, baseStatKey.version, allKey)
    list = list.:+((baseStatKey3, baseStatKey))
    val baseStatKey4 = BaseStatKey(baseStatKey.guid, baseStatKey.app, allKey, baseStatKey.version, baseStatKey.channel)
    list = list.:+((baseStatKey4, baseStatKey))
    val baseStatKey5 = BaseStatKey(baseStatKey.guid, baseStatKey.app, baseStatKey.plat, allKey, allKey)
    list = list.:+((baseStatKey5, baseStatKey))
    val baseStatKey6 = BaseStatKey(baseStatKey.guid, baseStatKey.app, baseStatKey.plat, allKey, baseStatKey.channel)
    list = list.:+((baseStatKey6, baseStatKey))
    val baseStatKey7 = BaseStatKey(baseStatKey.guid, baseStatKey.app, baseStatKey.plat, baseStatKey.version, allKey)
    list = list.:+((baseStatKey7, baseStatKey))
    val baseStatKey8 = BaseStatKey(baseStatKey.guid, baseStatKey.app, baseStatKey.plat, baseStatKey.version, baseStatKey.channel)
    list = list.:+((baseStatKey8, baseStatKey))
    list
  }

  implicit def calcBaseStatKey(baseStat: RDD[BaseStatKey]): RDD[(BaseStatResultKey, Long)] = {
    baseStat.flatMap(x => LogUtils.flatMap(x))
      .reduceByKey((x1, x2) => x1)
      .map(x => (BaseStatResultKey(x._1.app, x._1.plat, x._1.version, x._1.channel), 1L))
      .reduceByKey((x1, x2) => x1 + x2)
  }
}