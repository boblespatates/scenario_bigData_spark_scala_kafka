package elk_package
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

class SparkConsumer {

      // Créer une session Spark et un contexte Spark
      val spark = SparkSession.builder.appName("kafkacoutword").master("local[*]").getOrCreate()

      // Obtenir le contexte Spark de la Sparksession pour créer un contexte de diffusion continu
      val sc = spark.sparkContext
      // Créer le contexte de diffusion StreamingContext, à intervalle de 10 secondes
      val ssc = new StreamingContext(sc, Seconds(1))

      // Créez un DStream qui recevant les données texte du Kafkaserver(la ou vous aller ecrire: le producer).
      val kstream = KafkaUtils.createStream(ssc, "localhost:2181", "spark-streaming-consumer-group", Map("sparkelk" /*nom du topic*/ -> 1))

      val words = kstream.flatMap(x => x._2.split(","))
      val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)

      // Pour imprimer le résultat wordcount du flux
      wordCounts.print()
      ssc.start()
      ssc.awaitTermination()
}
