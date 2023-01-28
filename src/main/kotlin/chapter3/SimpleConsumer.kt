package chapter3.chapter3

import chapter3.logger
import org.apache.kafka.clients.consumer.KafkaConsumer
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

  companion object {
    val TOPIC_NAME = "test"
    val GROUP_ID = "test-group"
  }
}