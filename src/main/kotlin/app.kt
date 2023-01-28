package chapter3

import chapter3.chapter3.CustomPartitioner

fun main() {
  val testProducer = SimpleProducer()
  testProducer.produceBlocking("blocking-message", CustomPartitioner::class)
}