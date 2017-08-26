package observatory

import java.time.LocalDate

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, IntegerType}

/**
  * 1st milestone: data extraction
  */
object Extraction {
  //  @transient lazy val conf: SparkConf = new SparkConf()
  //    .setMaster("local")
  //    .setAppName("GlobalWarming")
  //    .set("spark.ui.port", "3080")
  //    .set("spark.ui.port", "3080")
  //    .set("spark.driver.bindAddress", "127.0.0.1")

  @transient lazy val sparkSession = SparkSession.builder.
    master("local")
    .appName("spark session example")
    .getOrCreate()
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  import sparkSession.sqlContext.implicits._

  val STATIONS = "/stations.csv"
  val YEARS = Range.Int(1975, 2015, 1)

  /**
    * @param year             Year number
    * @param stationsFile     Path of the stations resource file to use (e.g. "/stations.csv")
    * @param temperaturesFile Path of the temperatures resource file to use (e.g. "/1975.csv")
    * @return A sequence containing triplets (date, location, temperature)
    */
  def locateTemperatures(year: Year, stationsFile: String, temperaturesFile: String): Iterable[(LocalDate, Location, Temperature)] = {
    locateTemps(year, stationsFile, temperaturesFile)
      .map(r=>(LocalDate.of(r.ymd.year, r.ymd.month, r.ymd.day), r.location, r.temperature))
      .collect()
  }

  def locateTemps(year: Year, stationsFile: String, temperaturesFile: String): Dataset[Observation] = {
    val stations: Dataset[Station] = dsStations(stationsFile)
    val observations: Dataset[ObservationStation] = dsYear(temperaturesFile, year)

    stations.as("A")
      .joinWith(observations.as("B"), $"A.stationIdentifier" === $"B.stationIdentifier")
      .as[(Station, ObservationStation)]
      .map(r => Observation(r._1, r._2))
  }

  /**
    * @param records A sequence containing triplets (date, location, temperature)
    * @return A sequence containing, for each location, the average temperature over the year.
    */
  def locationYearlyAverageRecords(records: Iterable[(LocalDate, Location, Temperature)]): Iterable[(Location, Temperature)] = {
    val rdd = sparkSession.sparkContext.parallelize(records.toList)
    rdd.map(r=>(r._2,r._3)).aggregateByKey((0.0, 0))(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
      .mapValues(sumCount => 1.0 * sumCount._1 / sumCount._2)
      .collect

  }

  def dfStations(stationsFile: String): DataFrame = {
    val df = sparkSession.read.csv(stationsFile).toDF("stn", "wban", "latitude", "longitude")
    df.filter((r: Row) => r.getAs("latitude") != null && r.getAs("longitude") != null)
      .withColumn("lat", df("latitude").cast(DoubleType))
      .withColumn("lon", df("longitude").cast(DoubleType))
  }

  def dfYear(yearFile: String, year: Year): DataFrame = {
    val df = sparkSession.read.csv(yearFile).toDF("stn", "wban", "month", "day", "temperature")
    df.withColumn("year", functions.lit(year))
      .withColumn("month", df("month").cast(IntegerType))
      .withColumn("day", df("day").cast(IntegerType))
      .withColumn("temperature", df("temperature").cast(DoubleType))
  }


  def dsStations(stationsFile: String): Dataset[Station] = {
    dfStations(stationsFile).as[StationRaw].map(sr => sr.station())
  }

  def dsYear(yearFile: String, year: Year): Dataset[ObservationStation] = {
    dfYear(yearFile, year).as[ObservationRaw].map(or => or.observation())
  }
}
