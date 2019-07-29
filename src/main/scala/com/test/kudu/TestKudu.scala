package com.test.kudu

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.client.CreateTableOptions
import scala.collection.JavaConverters._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.types.StringType
import org.apache.kudu.client.AlterTableOptions
import org.apache.kudu.Type
import org.apache.kudu.client.KuduClient

case class Cust(id: Long, name: String)
object TestKudu {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("TestKudu")
      //      .master("spark://172.16.9.141:7077")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/spark-warehouse")
      .config("spark.hadoop.fs.defaultFS", "hdfs://master9102:8020")
      .getOrCreate()
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext

    val custs = Array(
      Cust(1, "龙利鱼"),
      Cust(2, "jane"),
      Cust(3, "jordan"),
      Cust(4, "enzo"),
      Cust(5, "laura"))
    import spark.implicits._
    val custsRDD = sc.parallelize(custs)
    val custDF = custsRDD.toDF()

    //new kudu客户端
    val kuduContext = new KuduContext("slave9114:7051", spark.sparkContext)

    //1.添加数据  这里使用的是impala中建的表
    //    kuduContext.insertRows(custDF, "impala::default.test_kudu_table")
    //2.删除数据  删除数据只需要主键即可,将需要删除的df转换成临时表,通过临时表获取需要删除的主键
    custDF.registerTempTable("kudu_table")
    val filteredDF = spark.sql("select id from kudu_table where id>=5")
    //    kuduContext.deleteRows(filteredDF, "impala::default.test_kudu_table")
    //3.更新数据
    val updateDF = sc.parallelize(Seq(Cust(2, "草菇"))).toDF()
    //    kuduContext.updateRows(updateDF, "impala::default.test_kudu_table")
    //4.upsert 数据
    //    kuduContext.upsertRows(custDF, "impala::default.test_kudu_table")

    //5.读取数据
    val df = spark.read
      //      .options(Map("kudu.master" -> "slave9114:7051", "kudu.table" -> "impala::default.test_kudu_table")) //操作impala建的表要写全称
      .options(Map("kudu.master" -> "slave9114:7051", "kudu.table" -> "test_kudu")) //操作spark建的表直接写表名称
      .format("kudu").load
    df.select("*").show()

    //6.spark 创建表     两种方式 :6.1根据现成的数据结构创建表   6.2自定义表结构
    val kuduTableSchema = createTable1(custDF)
    //    val kuduTableSchema = createTable2()
    //    kuduContext.createTable(
    //      "test_kudu", kuduTableSchema, Seq("id"),
    //      new CreateTableOptions()
    //        .setNumReplicas(1) // 设置副本数量
    //        .addHashPartitions(List("id").asJava, 2)) //设置分区 数据分到几台机器上

    //7.判断表是否存在
    val flag = kuduContext.tableExists("test_kudu")
    println(flag)

    //8.删除表
    //    kuduContext.deleteTable("test_kudu")
    val flag1 = kuduContext.tableExists("test_kudu")
    println(flag1)

    //9.表结构相关操作
    val client = new KuduClient.KuduClientBuilder("slave9114:7051").build()
    val ato = new AlterTableOptions
    //    ato.addColumn("height", Type.STRING, "170")//第三位默认值
    //    ato.renameColumn("height", "heightttt")
    //    ato.dropColumn("heightttt")
    //    client.alterTable("impala::default.test_kudu_table", ato)

    sc.stop()
  }

  //创建表
  def createTable1(df: DataFrame): StructType = {
    val kuduTableSchema = df.schema
    kuduTableSchema
  }

  def createTable2(): StructType = {
    val kuduTableSchema = StructType(
      StructField("id", dataType = LongType, nullable = false) ::
        StructField("name", StringType, true) :: Nil)
    kuduTableSchema
  }

}