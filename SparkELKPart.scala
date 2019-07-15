package elk_package

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.matching.Regex

class SparkELKPart(filepathAnimeList : String,
                      filepathUserAnimeList : String,
                      filepathUserList : String) {

  ////////////// ATTRIBUTES INITIALISATION /////////////////

  val spark = SparkSession.builder()
    .appName("Senario Big Data")
    .config("spark.master", "local")
    .getOrCreate()

  // This import enable us to call $"column_name"
  import spark.implicits._

  // Initialisation of the DataFrame AnimeList
  val dfAnimeList = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filepathAnimeList)


  // Initialisation of the DataFrame UserAnimeList
  val dfUserAnimeList = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filepathUserAnimeList)


  // Initialisation of the DataFrame UserList
  val dfUserList = spark.read.format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load(filepathUserList)


  //////////// METHOD DEFINITION /////////


  // Number of Anime watched by Users : question 2
  def animeByUser(dfUserAnimeList : DataFrame) : DataFrame = {
    dfUserAnimeList.select("username", "anime_id")
      .groupBy("username")
      .agg(countDistinct("anime_id"))
  }


  // The most watched Anime by User : question 3
  def mostWatchedAnimeByUser(dfUserAnimeList : DataFrame) : DataFrame = {

    dfUserAnimeList.select("username", "anime_id", "my_watched_episodes", "my_rewatching", "my_rewatching_ep")
      .orderBy(desc("my_rewatching")).show()

    dfUserAnimeList.select("username", "anime_id", "my_watched_episodes")
    /*
    dfUserAnimeList.select("username", "anime_id")
      .groupBy("username", "anime_id")
      .agg(count("anime_id"))
      //.agg(max("count(anime_id)"))

     */

    // Convert format : "24 min. per ep."
    // Into : Int(24)
    def durationEpisode(durationStr : String): Int =  {
      val numberPattern: Regex = "[0-9]+".r
      println(numberPattern)
      numberPattern.findFirstMatchIn(durationStr) match {
        case None => {println("this duration doesn't contain any number"+ durationStr)
          -1}
        case Some(m : Regex.Match) => s"$m".toInt
      }
    }

    // Add a clean column : clean duration
    // From : "24 min. per ep."
    // To : Int(24)
    val durationPerEpisodeUdf = udf(durationEpisode _)
    dfAnimeList.withColumn("clean_duration",
      durationPerEpisodeUdf($"duration")).show()
    dfAnimeList
  }

  // Most watched Episode by Anime : question 4
  def mostWatchedEpisodByAnime(dfUserAnimeList : DataFrame) : DataFrame = {
    val dfTemp = dfUserAnimeList.select("anime_id", "my_watched_episodes")
      .groupBy("anime_id", "my_watched_episodes")
      .agg(count("my_watched_episodes"))
      .orderBy(desc("count(my_watched_episodes)"))
        .distinct()

    dfTemp.select("anime_id", "my_watched_episodes", "count(my_watched_episodes)")
      .groupBy("anime_id", "my_watched_episodes")
      .agg(max("count(my_watched_episodes)"))
      .orderBy(desc("max(count(my_watched_episodes))"))
      .distinct()
  }

  // User who watched most number of anime : question 5
  def userWatchedMostAnime(dfUserAnimeList : DataFrame) : Any = {

    dfUserAnimeList.select("username", "anime_id")
      .groupBy("username")
      .agg(countDistinct("anime_id").as("nb_anime_watched"))
      .orderBy(desc("nb_anime_watched"))
      .head(1)(0)(0)
  }

  // User who watched most number of Episode : question 6
  def userWatchedMostEpisod(dfUserList : DataFrame) : Any = {
    dfUserList.select("username", "stats_episodes")
      .orderBy(desc("stats_episodes"))
      .head(1)(0)(0)
  }

  // How many different users is there : question 7
  def nbUsers(dfUserList : DataFrame) : DataFrame = {
    dfUserList.select(countDistinct("user_id"))
  }

  // Select the 100 which give the better mean grade : question 8
  def userBetterMean(dfUserList : DataFrame): DataFrame = {
    dfUserList.select("username", "stats_mean_score")
      .orderBy(desc("stats_mean_score"))
      .limit(100)
  }

  // Join the data frames : question 9
  def filesjoin(dfUserList : DataFrame,
                dfUserAnimeList : DataFrame,
                dfAnimeList : DataFrame) : DataFrame = {

    val cols = List("username", "gender", "location",
      "birth_date", "anime_id", "title_english", "duration",
      "rating", "score", "rank", "my_watched_episodes",
      "my_score", "my_status", "my_tags")

    dfAnimeList.join(dfUserAnimeList, Seq("username"))
      .join(dfUserList, Seq("anime_id"))
      .select(cols.head, cols.tail: _*)
  }

}
