package producer

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.net.URI
import java.net.http.{HttpClient, WebSocket}
import java.util.Properties
import java.util.concurrent.{CompletionStage, CountDownLatch}

object BinanceProducer {
  def main(args: Array[String]): Unit = {
    // 1. Configurer le producteur Kafka
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    // 2. Que faire à chaque message reçu de Binance
    val listener = new WebSocket.Listener {
      override def onText(ws: WebSocket, data: CharSequence, last: Boolean): CompletionStage[_] = {
        val message = data.toString
        producer.send(new ProducerRecord[String, String]("crypto-trades", message))
        println(s"→ Kafka : $message")
        ws.request(1)
        null
      }
    }

    // 3. Ouvrir le WebSocket vers Binance
    HttpClient.newHttpClient()
      .newWebSocketBuilder()
      .buildAsync(URI.create("wss://stream.binance.com:9443/ws/btcusdt@trade"), listener)
      .join()

    println("Connecté à Binance — réception des trades BTC en direct...")

    // 4. Garder le programme en vie
    new CountDownLatch(1).await()
  }
}