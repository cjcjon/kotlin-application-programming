package chapter3

fun main() {
  val testProducer = SimpleProducer()
  testProducer.produceNonBlocking("nonblocking-message")
}