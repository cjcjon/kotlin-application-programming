package chapter3.chapter3

import logger
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
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

  fun consumeAsync(): Unit {
    val configs = KafkaProperties.stringConsumer(groupId = GROUP_ID)
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(listOf(TOPIC_NAME))

    while (true) {
      val records = consumer.poll(Duration.ofSeconds(1))
      records.forEach { logger().info("{}", it) }
      consumer.commitAsync() { offsets, e ->
        when {
          e != null -> logger().error("Commit failed for offsets {}", offsets, e)
          else -> println("Commit succeeded")
        }
      }
    }
  }

  fun consumeRebalanced(): Unit {
    val configs = KafkaProperties.stringConsumer(groupId = GROUP_ID)
    configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)

    val consumer = KafkaConsumer<String, String>(configs)
    val currentOffsets = HashMap<TopicPartition, OffsetAndMetadata>()
    consumer.subscribe(listOf(TOPIC_NAME), object : ConsumerRebalanceListener {
      // 리밸런스 시작 전 호출
      override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>?) {
        logger().warn("Partitions are revoked")
        consumer.commitSync(currentOffsets)
      }

      // 리밸런스 종료 후 호출
      override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>?) {
        logger().warn("Partitions are assigned")
      }
    })

    while (true) {
      val records = consumer.poll(Duration.ofSeconds(1))
      records.forEach {
        logger().info("{}", it)
        currentOffsets.put(TopicPartition(it.topic(), it.partition()), OffsetAndMetadata(it.offset() + 1))
        consumer.commitSync(currentOffsets)
      }
    }
  }

  fun consumePartition(partitionNumber: Int): Unit {
    val configs = KafkaProperties.stringConsumer(groupId = GROUP_ID)

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.assign(listOf(TopicPartition(TOPIC_NAME, partitionNumber)))

    while (true) {
      val records = consumer.poll(Duration.ofSeconds(1))
      records.forEach { logger().info("{}", it) }
    }
  }

  fun consumeShutdownSafely(): Unit {
    val configs = KafkaProperties.stringConsumer(groupId = GROUP_ID)

    val consumer = KafkaConsumer<String, String>(configs)
    consumer.subscribe(listOf(TOPIC_NAME))

    // 런타임 종료될 때 컨슈머 종료 훅 호출
    Runtime.getRuntime().addShutdownHook(object : Thread() {
      override fun run() {
        logger().info("Shutdown hook")
        consumer.wakeup()
      }
    })

    try {
      while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        records.forEach { logger().info("{}", it) }
      }
    } catch (e: WakeupException) {
      logger().warn("Wakeup consumer")
    } finally {
      consumer.close()
    }
  }

  companion object {
    val TOPIC_NAME = "test"
    val GROUP_ID = "test-group"
  }
}