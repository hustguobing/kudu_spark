package com.test.kudu

import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu.KuduContext

case class TeachUv(lasttimestamp: Long, app: String,
                   plat:    String,
                   version: String,
                   guid:    String,
                   uid:     String,
                   count:   Long,
                   time:    Long)
object SaveUserAndViewTime2Kudu {
  def main(args: Array[String]): Unit = {
    val day = args(0)
    val spark = SparkSession.builder
      .appName(s"SaveUserAndViewTime2Kudu:${day}")
      .master("spark://172.16.9.141:7077")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    sc.setLogLevel("WARN")
    val viewPath = s"""/taf/warehouse/Base/AppStatServer/${day}/*"""

    val data = sc.union(sc.textFile(viewPath, 1)).filter(x => {
      (x.contains("count001") && x.contains("apptime001")) || (x.contains("1000001"))
    }).map(x => {
      val eventLogRecord = EventLogParse.apply(x)
      eventLogRecord
    }).filter(_.isDefined)
      .map(x => {
        val event = x.get
        var count = 0L
        var time = 0L
        if (event.eventKeyValues.contains(EventKey("count001", day, 4))) {
          val countKey = EventKey("count001", day, 4)
          count = event.eventKeyValues.getOrElse(countKey, EventValue(1L, 0L, 0L)).totalValue
          val timeKey = EventKey("apptime001", day, 4)
          time = event.eventKeyValues.getOrElse(timeKey, EventValue(1L, 0L, 0L)).totalValue
        } else {
          count = 1L
          val timeKey = EventKey("1000001", day, 4)
          time = event.eventKeyValues.getOrElse(timeKey, EventValue(1L, 0L, 0L)).totalValue
        }
        ((event.app, event.plat, event.version, event.guid, event.uid), (count, time, event.serverTimestamp))
      }).reduceByKey((x1, x2) => {
        val count = x1._1 + x2._1
        val time = x1._2 + x2._2
        val timeStamp = if (x1._3 > x2._3) { x1._3 } else { x2._3 }
        (count, time, timeStamp)
      }).map(x => TeachUv(x._2._3, x._1._1, x._1._2, x._1._3, x._1._4, x._1._5, x._2._1, x._2._2))
      .filter(x => x.app.equals("stock"))

    import spark.implicits._ //隐式转换成df
    val df = data.toDF()
    df.registerTempTable("kudu_table")
    val filteredDF = spark.sql("select lasttimestamp from kudu_table")

    println(filteredDF.count())
    val kuduContext = new KuduContext("slave9114:7051", spark.sparkContext)

    //    kuduContext.insertRows(df, "impala::default.teach_uv_kudu")
    //    kuduContext.upsertRows(df, "impala::default.teach_uv_kudu")
    //    kuduContext.deleteRows(filteredDF, "impala::default.teach_uv_kudu") //删除数据只需要主键,通过注册成临时表实现,然后获取
    //更新数据
    val customers = Array(
      TeachUv(1561998491000L, "teach", "IOS", "2.1.0", "a7806186ceb44f75b8292ca773e50a52", "up1682668", 100, 10000))
    val customersRDD = sc.parallelize(customers)
    val customersDF = customersRDD.toDF()
    kuduContext.updateRows(customersDF, "impala::default.teach_uv_kudu") //直接传入整个df就可以更新

    println("完成")
    sc.stop()
  }
}