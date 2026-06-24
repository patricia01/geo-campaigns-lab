package producer

import model.GeoEvent
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.time.Instant
import java.util.Properties
import scala.util.Random

/**
 * =============================================================
 *  PRODUCTOR DE EVENTOS — YA COMPLETADO
 *  No es necesario modificar este fichero.
 *
 *  mvn exec:exec -Dexec.mainClass="producer.GeoEventProducer"
 * =============================================================
 *
 *  Simula clientes moviéndose por la red:
 *    - t=0 a t=60s  : tráfico normal, congestión baja (20-50%)
 *    - t=60s en adelante: EVENTO MASIVO en CELL_003 y CELL_007
 *      → congestión sube al 75-95% → dispara campañas 5G
 *
 *  También inyecta ~5% de eventos sucios (campos nulos, duplicados)
 *  para que practiques la limpieza en el pipeline.
 */
object GeoEventProducer extends App {

  val TOPIC = "geo-events"
  val DELAY = 200L

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer",    "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer",  "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks",              "1")

  val producer = new KafkaProducer[String, String](props)
  val rnd      = new Random()

  val cells = Map(
    "CELL_001" -> (40.4168, -3.7038, "Madrid Centro"),
    "CELL_002" -> (40.4530, -3.6883, "Madrid Norte"),
    "CELL_003" -> (40.4153, -3.6844, "Madrid Retiro"),
    "CELL_004" -> (41.3851,  2.1734, "Barcelona Eixample"),
    "CELL_005" -> (41.4036,  2.1744, "Barcelona Gràcia"),
    "CELL_006" -> (39.4699, -0.3763, "Valencia Centro"),
    "CELL_007" -> (40.4089, -3.6916, "Madrid Bernabéu"),
    "CELL_008" -> (37.3891, -5.9845, "Sevilla Centro"),
    "CELL_009" -> (38.3452, -0.4810, "Alicante Centro"),
    "CELL_010" -> (36.7213, -4.4214, "Málaga Costa")
  )

  val clients     = (1 to 50).map(i => f"CLIENT_$i%04d")
  var clientCells = clients.map(c => c -> cells.keys.toSeq(rnd.nextInt(cells.size))).toMap
  var msgCount    = 0
  var eventMode   = false

  println(
    """
      |╔══════════════════════════════════════════════════════╗
      |║       GEO CAMPAIGNS LAB — PRODUCTOR                 ║
      |║  t+60s → EVENTO MASIVO en CELL_003 y CELL_007       ║
      |║  ~5% eventos sucios (nulos + duplicados)            ║
      |╚══════════════════════════════════════════════════════╝
      |""".stripMargin)

  sys.addShutdownHook { println("\n[Producer] Cerrando..."); producer.close() }

  while (true) {
    msgCount += 1

    if (msgCount == 300 && !eventMode) {
      eventMode = true
      println("\n🔴 EVENTO MASIVO ACTIVADO — CELL_003 y CELL_007 saturándose...\n")
    }

    if (rnd.nextDouble() < 0.1) {
      val c = clients(rnd.nextInt(clients.length))
      clientCells = clientCells.updated(c, cells.keys.toSeq(rnd.nextInt(cells.size)))
    }

    val clientId         = clients(rnd.nextInt(clients.length))
    val cellId           = clientCells(clientId)
    val (lat, lon, _)    = cells(cellId)
    val dirty            = rnd.nextDouble() < 0.05
    val cong             = computeCongestion(cellId, eventMode)

    val event: java.util.Map[String, Any] = {
      val m = new java.util.LinkedHashMap[String, Any]()
      if (dirty && rnd.nextBoolean()) {
        m.put("clientId",   null)
        m.put("cellId",     cellId)
        m.put("latitude",   lat)
        m.put("longitude",  lon)
        m.put("signalDbm",  null)
        m.put("congestion", cong)
        m.put("technology", "4G")
        m.put("timestamp",  Instant.now().toEpochMilli)
      } else {
        m.put("clientId",   clientId)
        m.put("cellId",     cellId)
        m.put("latitude",   lat + (rnd.nextDouble() - 0.5) * 0.01)
        m.put("longitude",  lon + (rnd.nextDouble() - 0.5) * 0.01)
        m.put("signalDbm",  -70 - rnd.nextInt(30))
        m.put("congestion", cong)
        m.put("technology", if (rnd.nextDouble() > 0.4) "5G" else "4G")
        m.put("timestamp",  Instant.now().toEpochMilli + (if (dirty) -100 else 0))
      }
      m
    }

    val json   = mapper.writeValueAsString(event)
    val key    = if (event.get("clientId") == null) "null_key" else event.get("clientId").toString
    val record = new ProducerRecord[String, String](TOPIC, key, json)
    producer.send(record)

    val icon = if (dirty) "💀" else if (cong >= 75) "🔴" else if (cong >= 50) "🟡" else "🟢"
    println(s"$icon ${key.padTo(12,' ')} | ${cellId.padTo(9,' ')} | cong: $cong%${if (dirty) " [SUCIO]" else ""}")

    Thread.sleep(DELAY)
  }

  def computeCongestion(cellId: String, eventMode: Boolean): Int = {
    val base = cellId match {
      case "CELL_003" if eventMode => 75 + rnd.nextInt(20)
      case "CELL_007" if eventMode => 70 + rnd.nextInt(25)
      case _                       => 20 + rnd.nextInt(50)
    }
    math.min(base, 100)
  }
}
