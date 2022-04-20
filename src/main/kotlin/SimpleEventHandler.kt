import com.launchdarkly.eventsource.EventHandler
import com.launchdarkly.eventsource.MessageEvent
import java.util.UUID

class SimpleEventHandler<K, V>(
    private val publisher: EventPublisher<K, V>
) : EventHandler {
    override fun onOpen() {
        TODO("Not yet implemented")
    }

    override fun onClosed() {
        TODO("Not yet implemented")
    }

    override fun onMessage(event: String?, messageEvent: MessageEvent?) {
        if (event == "message") {
            publisher.publishMessage(UUID.randomUUID() as K, messageEvent?.data as V)
        }
    }

    override fun onComment(comment: String?) {
        TODO("Not yet implemented")
    }

    override fun onError(t: Throwable?) {
        TODO("Not yet implemented")
    }
}