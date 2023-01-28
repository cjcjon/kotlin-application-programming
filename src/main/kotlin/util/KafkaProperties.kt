package util

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

object KafkaProperties {
  private val BOOTSTRAP_SERVERS = "localhost:9092"

  fun stringProducer(servers: String = BOOTSTRAP_SERVERS): Properties {
    val configs = Properties()
    configs.putAll(
      mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to servers,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.qualifiedName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.qualifiedName
      )
    )

    return configs
  }

  fun stringConsumer(servers: String = BOOTSTRAP_SERVERS, groupId: String): Properties {
    val configs = Properties()
    configs.putAll(
      mapOf(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to servers,
        ConsumerConfig.GROUP_ID_CONFIG to groupId,
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.qualifiedName,
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.qualifiedName
      )
    )

    return configs
  }
}