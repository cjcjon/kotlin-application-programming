package chapter3

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import util.KafkaProperties
import kotlin.reflect.KClass

class SimpleProducer {
  fun produce(message: String): Unit {
    val configs = KafkaProperties.stringProducer()

    val producer = KafkaProducer<String, String>(configs)
    val record = ProducerRecord<String, String>(TOPIC_NAME, message)

    logger().info("{}", record)
    producer.send(record)
    producer.flush()
    producer.close()
  }

  fun produceBlocking(message: String, partitioner: KClass<out Partitioner>): Unit {
    val configs = KafkaProperties.stringProducer()
    configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, partitioner.java)

    val producer = KafkaProducer<String, String>(configs)
    val record = ProducerRecord<String, String>(TOPIC_NAME, "Pangyo", message)
    val metaData = producer.send(record).get()
    logger().info(metaData.toString())

    producer.close()
  }

  companion object {
    val TOPIC_NAME = "test"
  }
}