package chapter3.chapter3

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.utils.Utils

class CustomPartitioner : Partitioner {

  override fun partition(
    topic: String?,
    key: Any?,
    keyBytes: ByteArray?,
    value: Any?,
    valueBytes: ByteArray?,
    cluster: Cluster
  ): Int {
    val nonNullableKeyBytes = keyBytes ?: throw InvalidRecordException("Need message key")

    if (KEY_PANGYO == (key as? String ?: nonNullableKeyBytes.contentToString())) {
      return PARTITION_PANGYO
    } else {
      val partitions = cluster.partitionsForTopic(topic)
      val numPartitions = partitions.size
      return Utils.toPositive(Utils.murmur2(nonNullableKeyBytes)) % numPartitions
    }
  }

  override fun configure(configs: MutableMap<String, *>?) {
    // 사용 X
  }

  override fun close() {
    // 사용 X
  }

  companion object {
    val KEY_PANGYO = "Pangyo"
    val PARTITION_PANGYO = 0
  }
}