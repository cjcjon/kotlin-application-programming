package util

import org.apache.kafka.clients.producer.ProducerConfig
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
}