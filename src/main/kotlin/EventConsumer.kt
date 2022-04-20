import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.Deserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties

class EventConsumer<K, V>(
    private val bootstrapServer: String,
    private val topic: String,
    private val group: String,
    private val keyDeserializer: Deserializer<K>,
    private val messageDeserializer: Deserializer<V>
) {

    private val properties = Properties()
    private var consumer: KafkaConsumer<K, V>
    private val logger: Logger = LoggerFactory.getLogger(EventPublisher::class.java)

    init {
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer::class.qualifiedName)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, messageDeserializer::class.qualifiedName)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = KafkaConsumer<K, V>(properties)
        consumer.subscribe(listOf(topic))
    }

    fun consumeMessages() {
        try {
            while (true) {
                val consumerRecords: ConsumerRecords<K, V> = consumer.poll(Duration.ofMillis(100))
                for (consumerRecord in consumerRecords) {
                    logger.info(
                        "Getting consumer record key: ${consumerRecord.key()} \t value: ${consumerRecord.value()}"
                    )
                }
            }
        } catch (e: WakeupException) {
            logger.info("Consumer poll woke up")
        } finally {
            consumer.close()
        }
    }

    fun shutdown() {
        runCatching { consumer.wakeup() }.onFailure { e -> logger.error("Error shutting down consumer: ${e.message}") }
    }
}