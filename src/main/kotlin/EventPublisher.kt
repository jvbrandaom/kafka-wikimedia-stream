import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.Serializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties

class EventPublisher<K, V>(
    private val bootstrapServer: String,
    private val topic: String,
    private val keySerializer: Serializer<K>,
    private val messageSerializer: Serializer<V>
) {
    private val properties = Properties()
    private var producer: KafkaProducer<K, V>
    private val logger: Logger = LoggerFactory.getLogger(EventPublisher::class.java)

    init {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer::class.qualifiedName)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, messageSerializer::class.qualifiedName)
        producer = KafkaProducer<K, V>(properties)
    }

    fun publishMessage(key: K, message: V) {
        val record = ProducerRecord(topic, key, message)
        producer.send(record) { _, e ->
            e?.let { logger.error("error publishing message: $message") } ?: logger.info("published message: $message")
        }
    }

    fun close() {
        runCatching { producer.close() }.onFailure { e -> logger.error(e.message) }
    }
}