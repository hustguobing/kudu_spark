package com.test.kudu

/**
 * 解析事件日志
 */
case class EventLogRecord(guid: String, serverTimestamp: Long, serverIp: String, app: String,
                          version: String, channel: String, plat: String, uid: String, eventKeyValues: Map[EventKey, EventValue])
case class EventKey(key: String, subKey: String, keyType: Int)
case class EventValue(totalUser: Long, totalCount: Long, totalValue: Long)
object EventLogParse {
  def main(args: Array[String]) {
    //    println(apply("10.251.255.25|2019-04-24 18:42:32|H5|9b3a68a9d26e625d37624360eae7598d|SN=ADR_com.upchina.teach&VN=19032715_1.0.3_19032715_GA&MO=VOG-AL10&VC=HUAWEI&RV=HUAWEI_136C00R1&OS=28&CHID=50007_50007&MN=teach|100.120.35.86|up1433804|4,TAF_UI_CLICK;2003,1").get)
    //    println(apply("10.251.250.225|2019-05-06 19:13:02|kdp|b026cb639e02d688eaa77cffde528c55|SN=ADR_com.upchina.kdp&VN=0_0.0.1_0&MO=COL-AL10&VC=HONOR&RL=0_0&RV=&OS=9&CHID=0_62001&MN=kdp&SDK=||up0003294|1,dinggupiao001,1,0").get)
    //    println(apply("10.251.255.25|2019-06-03 14:20:35|H5|352058d90683b8014aebea6bba090bd6|SN=ADR_com.upchina&VN=19051091_6.1.8_19051091_GA&MO=Redmi 6A&VC=Xiaomi&RV=xiaomi_V10.3.3.0.OCBCNXM&OS=27&CHID=9510_9510&MN=stock|100.121.118.167||4,TAF_UI_CLICK;2002,2").get)
    println(apply("10.251.250.225|2019-06-24 17:46:49|teach|54e896fc67f3b765b7ed8d5cff1e5da0|SN=IOS_com.upchina.ios.zngp&VN=19061805_2.1.0_19061805_GA&MO=iPhone 8 Plus&VC=Apple&RL=0_0&RV=&OS=12.3.2&CHID=55026_55026&MN=teach|100.121.118.220|up0739518|4,TAF_UI_ENTER;qn,1;qncdgc,1;qnqnng,1;qnzqcw,1;sy,1|4,apptime001;20190624,148|4,count001;20190624,1|4,qntime001;20190624,88|4,sytime001;20190624,2").get)
    println(apply("10.251.250.225|2019-06-20 16:55:58|stock|935a1daea89b22e968d9bfce38c92a7e|SN=IOS_com.upchina.ios.advisor&VN=19061217_1.1.8_19061217_GA&MO=iPhone 5s&VC=Apple&RL=0_0&RV=&OS=12.1.1&CHID=40001_40001&MN=advisor|100.120.35.80|up1682113|4,1000001;20190620,318|4,1000002;20190620,1|4,TAF_UI_CLICK;1016044,1;1016057,1|4,TAF_UI_ENTER;1006,4;1008,1").get)

  }
  def apply(line: String): Option[EventLogRecord] = {
    try {
      val splits = line.split("\\|");
      val length = splits.length;
      if (length > 7) {
        val serverIp = splits(0)
        val serverTime = splits(1)
        val bid = splits(2)
        val serverTimestamp = LogUtils.parseTimestamp(serverTime)
        val guid = splits(3);
        val xua = splits(4);
        val uid = splits(6);
        val xuaOption = LogUtils.parseXuaALL(xua);
        if (xuaOption.isDefined) {
          val (app, plat, os, version, channel, mo, vc, width, height, pkgName) = xuaOption.get
          val eventKeyValueList = 7.until(length).flatMap(x => {
            var list = Seq[(EventKey, EventValue)]()
            val eventStr = splits(x)
            if (eventStr.indexOf(";") < 0) {
              val eventSplits = eventStr.split(",");
              if (eventSplits.length > 2) {
                val keyType = eventSplits(0).toInt;
                val eventKey = eventSplits(1);
                val keyValue = {
                  val tmpValue = eventSplits(2).toLong;
                  if (tmpValue > 0 && tmpValue < 86400000) tmpValue else 0L
                }
                val keyCount = if (keyType == 1 && eventSplits.length == 4) eventSplits(3).toLong else 0L
                val elem = (EventKey(eventKey, "", keyType), EventValue(1L, keyCount, keyValue))
                list = list.:+(elem)
              }
            } else {
              val eventSplits = eventStr.split(";");
              if (eventSplits.length >= 2) {
                val head = eventSplits(0);
                val headSplits = head.split(",")
                if (headSplits.length == 2) {
                  val keyType = headSplits(0).toInt
                  val eventKey = headSplits(1);
                  if (keyType == 3 || keyType == 4) {
                    1.until(eventSplits.length).foreach(x => {
                      val eventSubStr = eventSplits(x);
                      val eventSubStrSplits = eventSubStr.split(",")
                      if ((keyType == 3 && eventSubStrSplits.length == 3) || ((keyType == 4 && eventSubStrSplits.length == 2))) {
                        val subKey = eventSubStrSplits(0);
                        val keyValue = {
                          val value = eventSubStrSplits(1).toLong
                          if (value > 0 && value < 86400000) value else 0L
                        }
                        val keyCount = {
                          if (keyType == 3) eventSubStrSplits(2).toInt else 0L
                        }
                        val elem = (EventKey(eventKey, subKey, keyType), EventValue(1L, keyCount, keyValue))
                        list = list.:+(elem)
                      }
                    })
                  }
                }
              }
            }
            list
          })
          Option(EventLogRecord(guid, serverTimestamp, serverIp, bid,
            version, channel, plat, uid, eventKeyValueList.toMap))
        } else {
          Option[EventLogRecord](null)
        }
      } else {
        Option[EventLogRecord](null)
      }
    } catch {
      case e: Throwable => Option[EventLogRecord](null)
    }
  }
}