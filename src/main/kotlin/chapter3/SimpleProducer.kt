package chapter3

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import util.KafkaProperties

class SimpleProducer {
  fun produce(message: String) {
    val configs = KafkaProperties.stringProducer()

    val producer = KafkaProducer<String, String>(configs)
    val record = ProducerRecord<String, String>(TOPIC_NAME, message)

    logger().info("{}", record)
    producer.send(record)
    producer.flush()
    producer.close()
  }

  companion object {
    val TOPIC_NAME = "test"
  }
}