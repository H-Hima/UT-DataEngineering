package ir.de2019_spark_streaming

import java.util.Properties
import java.util.concurrent.TimeoutException

import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer.{Callback, ProducerRecord, RecordMetadata, KafkaProducer => KP}
import java.util.Properties

import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.log4j.Logger

object KafkaProducer extends Serializable {

  val brokers: String = "localhost:9092"
  var properties: java.util.Properties = new Properties()

  properties.put("bootstrap.servers", brokers)
  properties.put("acks", "-1")
  properties.put("producer.type", "async")
  properties.put("client.id", "ZanjirChainsScalaProducer")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  properties.put(ProducerConfig.RETRIES_CONFIG, 3.toString)
  properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 60000.toString)

  val producer = new KP[String, String](properties)

  def send(topic:String, token: String): Unit = {
    val data = new ProducerRecord[String, String](topic, null, token)

    val ack=producer.send(data, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if(exception!=null) {
          println("Exception Thrown: ")
          exception.printStackTrace()
        }
        if (metadata == null) {
          println("No metadata received for this packet")
        }
      }
    })
  }
}
