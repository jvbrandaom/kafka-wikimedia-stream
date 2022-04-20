import com.launchdarkly.eventsource.EventSource
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.common.serialization.UUIDDeserializer
import org.apache.kafka.common.serialization.UUIDSerializer
import java.net.URI
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch


fun main(args: Array<String>) {
    val keySerializer = UUIDSerializer()
    val messageSerializer = StringSerializer()
    val publisher = EventPublisher<UUID, String>(args[0], args[1], keySerializer, messageSerializer)
    val eventHandler = SimpleEventHandler(publisher)
    val url = String.format("https://stream.wikimedia.org/v2/stream/recentchange")
    val eventSource: EventSource = EventSource.Builder(eventHandler, URI.create(url))
        .reconnectTime(Duration.ofMillis(3000))
        .build()
    val consumer = EventConsumer<UUID, String>(
        args[0], args[1], args[2], UUIDDeserializer(), StringDeserializer()
    )
    val countDownLatch = CountDownLatch(1)

    Runtime.getRuntime().addShutdownHook(Thread {
        publisher.close()
        consumer.shutdown()
        countDownLatch.countDown()
    })


    eventSource.start()
    consumer.consumeMessages()

    countDownLatch.await()
}