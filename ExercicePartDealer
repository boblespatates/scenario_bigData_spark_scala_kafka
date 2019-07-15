package elk_package

import org.apache.spark.sql.DataFrame

class ExercicePartDealer(filepathAnimeList: String,
                         filepathUserAnimeList : String,
                         filepathUserList : String) {

  // Intialisation of the spark ELK Object
  val sparkELKPart = new SparkELKPart(filepathAnimeList,
    filepathUserAnimeList,
    filepathUserList)

  val dfAnimeList = sparkELKPart.dfAnimeList
  val dfUserAnimeList = sparkELKPart.dfUserAnimeList
  val dfUserList = sparkELKPart.dfUserList

  // Display the result of the methods defined to answer the  ELK senario
  def sparkPartShow(sparkELKPart: SparkELKPart) : Unit = {

    // DataFrame initialisation
    val dfAnimeList = sparkELKPart.dfAnimeList
    val dfUserAnimeList = sparkELKPart.dfUserAnimeList
    val dfUserList = sparkELKPart.dfUserList

    // sparkELKPart.animeByUser(dfUserAnimeList).show() // question 2
    // sparkELKPart.mostWatchedAnimeByUser(dfUserAnimeList) // question 3
    // sparkELKPart.mostWatchedEpisodByAnime(dfUserAnimeList).show() // question 4
    // println(sparkELKPart.userWatchedMostAnime(dfUserAnimeList))// question 5
    // println(sparkELKPart.userWatchedMostEpisod(dfUserList)) // question 6
    // sparkELKPart.nbUsers(dfUserList).show() // question 7
    // sparkELKPart.userBetterMean(dfUserList).show()  // question 8
    // sparkELKPart.filesjoin(dfAnimeList, dfUserAnimeList, dfUserList).show() // question 9
  }

  // TODO : Change this, it not clean
  val topic = "senarioelk"

  // Write a DataFrame to Kafka
  def writeDF2Kafka(topic: String, df : DataFrame) : Unit = {
    val produceur = new Produceur()
    produceur.writeToKafka(topic, df)
  }

  writeDF2Kafka(topic, sparkELKPart.filesjoin(dfAnimeList, dfUserAnimeList, dfUserList))

  // Take data from kafka
  //val sparkConsumer = new SparkConsumer()


}
