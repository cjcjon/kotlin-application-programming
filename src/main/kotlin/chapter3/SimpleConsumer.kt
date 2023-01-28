package chapter3.chapter3

import chapter3.logger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import util.KafkaProperties
import java.time.Duration

class SimpleConsumer {
  fun consume(): Unit {
    val configs = KafkaProperties.stringConsumer(groupId = GROUP_ID)

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(listOf(TOPIC_NAME))

    while (true) {
      val records = consumer.poll(Duration.ofSeconds(1))
      records.forEach { logger().info("{}", it) }
    }
  }

  fun consumeSync(): Unit {
    val configs = KafkaProperties.stringConsumer(groupId = GROUP_ID)
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(listOf(TOPIC_NAME))

    while (true) {
      val records = consumer.poll(Duration.ofSeconds(1))
      records.forEach { logger().info("{}", it) }
      consumer.commitSync()
    }
  }

  fun consumeSyncOneByOne(): Unit {
    val configs = KafkaProperties.stringConsumer(groupId = GROUP_ID)
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(listOf(TOPIC_NAME))

    while (true) {
      val records = consumer.poll(Duration.ofSeconds(1))
      records.forEach {
        logger().info("{}", it)
        consumer.commitSync(
          mapOf(
            TopicPartition(it.topic(), it.partition()) to OffsetAndMetadata(it.offset() + 1)
          )
        )
      }
    }
  }

  companion object {
    val TOPIC_NAME = "test"
    val GROUP_ID = "test-group"
  }
}