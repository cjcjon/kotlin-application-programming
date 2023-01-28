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

  fun produceNonBlocking(message: String): Unit {
    val configs = KafkaProperties.stringProducer()

    val producer = KafkaProducer<String, String>(configs)
    val record = ProducerRecord<String, String>(TOPIC_NAME, message)
    producer.send(record) { metaData, e ->
      if (e != null) logger().error(e.message, e)
      else logger().info(metaData.toString())
    }

    // 비동기 수행을 위해 임시로 대기
    Thread.sleep(1000)
    producer.close()
  }

  companion object {
    val TOPIC_NAME = "test"
  }
}