package elk_package

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.DataFrame


class Produceur  extends Serializable {

  def writeToKafka(topic: String, dataFrame: DataFrame): Unit = {
    val configProduceur = new Properties()
    configProduceur.put("bootstrap.servers", "localhost:9092")
    configProduceur.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    configProduceur.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    lazy val producer = new KafkaProducer[String, String](configProduceur)

    dataFrame.foreach { row =>
      val monrecordtosend: ProducerRecord[String, String] = new ProducerRecord(topic, row.mkString(","))
      producer.send(monrecordtosend)
    }
    producer.close()
  }

}
