package chapter3

import chapter3.chapter3.KafkaAdmin

fun main() {
  val admin = KafkaAdmin()
  admin.describeBroker()
  admin.describeTopic()
}