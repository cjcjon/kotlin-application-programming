package chapter3.chapter3

import logger
import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.common.config.ConfigResource
import util.KafkaProperties

class KafkaAdmin {

  fun describeBroker(): Unit {
    val configs = KafkaProperties.defaultAdmin()
    val admin = AdminClient.create(configs)

    logger().info("== Get broker information")
    admin.describeCluster().nodes().get().forEach {
      logger().info("node : {}", it)

      val configResource = ConfigResource(ConfigResource.Type.BROKER, it.idString())
      val describeConfigs = admin.describeConfigs(listOf(configResource))
      describeConfigs.all().get().forEach { (_, config) ->
        run {
          config.entries().forEach { configEntry -> logger().info("${configEntry.name()} = ${configEntry.value()}") }
        }
      }
    }

    admin.close()
  }

  fun describeTopic(): Unit {
    val configs = KafkaProperties.defaultAdmin()
    val admin = AdminClient.create(configs)
    val topicNames = admin.describeTopics(listOf("test")).all().get()

    logger().info("== Get test topic information")
    logger().info("{}", topicNames)

    admin.close()
  }
}