import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
object Test1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Provider ranking")
      .set("spark.debug.maxToStringFields", "100")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)

    val df = sqlContext.read.format("csv").option("header", "true").load("data\\2019").withColumnRenamed("STN---", "STN_NO")
    df.show(false)
    val country_list_df = sqlContext.read.format("csv").option("header","true").load("data\\misc\\countrylist.csv")

    country_list_df.printSchema()

    val station_list_df = sqlContext.read.format("csv").option("header","true").load("data\\misc\\stationlist.csv")

    station_list_df.printSchema()

    val country_station_df = country_list_df.join(station_list_df,"COUNTRY_ABBR")
    country_station_df.show(false)

    val df_combi_cn_station = df.join(country_station_df, "STN_NO" ).drop("COUNTRY_ABBR")
    df_combi_cn_station.printSchema()
    val df_agg = df_combi_cn_station.withColumn("MAX", when(col("MAX").equalTo("9999.9"), "")
      .otherwise(col("MAX")))
    .withColumn("MIN", when(col("MIN").equalTo("9999.9"), "")
      .otherwise(col("MIN")))
      .groupBy("COUNTRY_FULL").agg(avg("MAX").as("max_temp_avg")
    ,avg("MIN").as("min_temp_avg"))
    val max_hot_temp = df_agg.select("COUNTRY_FULL","max_temp_avg").orderBy(col("max_temp_avg").desc).collect()(0)
    print(max_hot_temp)
    val max_cold_temp = df_agg.select("COUNTRY_FULL","min_temp_avg").orderBy(col("min_temp_avg")).collect()(0)
    print(max_cold_temp)
    df_agg.groupBy("COUNTRY_FULL").agg(max("max_temp_avg"),min("min_temp_avg"))

    val df_combi = df_combi_cn_station.select("COUNTRY_FULL","WDSP").withColumn("WDSP", when(col("WDSP").equalTo("999.9"),"").otherwise(col("WDSP")))
      .groupBy("COUNTRY_FULL").agg(avg("WDSP").as("avg_wdsp"))

    val avg_wdsp_cnt = df_combi.orderBy(col("avg_wdsp").desc)
    val li = avg_wdsp_cnt.collect()(1)
    print(li)


    val last_oper = df_combi_cn_station.select("COUNTRY_FULL","YEARMODA","FRSHTT")
    val tornado = last_oper.withColumn("tornado", substring(col("FRSHTT"), -1, 1))
      .filter(col("tornado") ===1)
      .withColumn("YEARMODA", from_unixtime(unix_timestamp(col("YEARMODA"), "yyyyMMdd"), "yyyy-MM-dd"))
    val tornodaByTime = Window.
      partitionBy("COUNTRY_FULL").
      orderBy("YEARMODA")

    val w1 = Window.partitionBy("COUNTRY_FULL").orderBy("YEARMODA")
    val w2 = Window.partitionBy("COUNTRY_FULL","tornado").orderBy("YEARMODA")
    val res = tornado.withColumn("grp", row_number.over(w1) - row_number.over(w2))


    val w3 = Window.partitionBy("COUNTRY_FULL","tornado","grp").orderBy("YEARMODA")
    val streak_res = res.withColumn("streak_0",when(col("tornado") === 1,0).otherwise(row_number().over(w3)))
      .withColumn("streak_1",when(col("tornado") === 0,0).otherwise(row_number().over(w3)))

    val win_streak = Window.partitionBy("COUNTRY_FULL").orderBy(col("streak_1").desc)
    val streak = streak_res.withColumn("row",row_number.over(win_streak))
      .where(col("row") === 1).drop("row")


    val highest_con_days = streak.select("COUNTRY_FULL","streak_1").orderBy(col("streak_1").desc).collect()(0)
    print(highest_con_days)

  }
}
