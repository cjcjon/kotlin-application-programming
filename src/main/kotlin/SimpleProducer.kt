import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties

class SimpleProducer {
  fun produce(message: String) {
    val configs = Properties()
    configs.putAll(
      mapOf(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to BOOTSTRAP_SERVERS,
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.qualifiedName,
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.qualifiedName
      )
    )

    val producer = KafkaProducer<String, String>(configs)
    val record = ProducerRecord<String, String>(TOPIC_NAME, message)

    logger().info("{}", record)
    producer.send(record)
    producer.flush()
    producer.close()
  }

  companion object {
    val TOPIC_NAME = "test"
    val BOOTSTRAP_SERVERS = "localhost:9092"
  }
}