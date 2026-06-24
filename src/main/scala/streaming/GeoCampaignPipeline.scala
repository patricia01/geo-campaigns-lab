package streaming

import ai.CampaignDecisionAI
import model.{CampaignAlert, GeoEvent}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types._

import java.time.Instant
import java.util.concurrent.TimeUnit

/**
 * =============================================================
 *  PASO 4 — PIPELINE PRINCIPAL
 *
 *  mvn compile exec:exec -Dexec.mainClass="streaming.GeoCampaignPipeline"
 *
 *  Este fichero es el núcleo de la práctica.
 *  Algunos bloques están dados, otros tienes que completarlos.
 *  Sigue los TODO en orden numérico.
 * =============================================================
 */
object GeoCampaignPipeline extends App {


  // ════════════════════════════════════════════════════════════
  //  BLOQUE A — SparkSession — YA DADO
  // ════════════════════════════════════════════════════════════
  val spark = SparkSession.builder()
    .appName("GeoCampaignStreaming")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  println(
    """
      |╔══════════════════════════════════════════════════════════╗
      |║         GEO CAMPAIGNS LAB — PIPELINE PRINCIPAL          ║
      |║  ① Data Engineering → limpieza + deduplicación         ║
      |║  ② Data Science     → ventana + join + IA              ║
      |║  ③ Visualización    → CSV + Parquet                    ║
      |╚══════════════════════════════════════════════════════════╝
      |""".stripMargin)

  // ════════════════════════════════════════════════════════════
  //  BLOQUE B — Cargar tablas de referencia desde Parquet
  //             YA DADO — observa cómo se cargan tablas estáticas
  // ════════════════════════════════════════════════════════════
  // En streaming podemos hacer join con DataFrames estáticos.
  // Se cargan una sola vez al arrancar el pipeline.

  val consentsDF = ???
  val cellsDF    = ???

  println(s"[Setup] Consentimientos: ${consentsDF.count()} clientes")
  println(s"[Setup] Celdas objetivo: ${cellsDF.count()} celdas\n")

  // ════════════════════════════════════════════════════════════
  //  BLOQUE C — Schema del evento JSON — YA DADO
  //  Importante: los campos que pueden llegar nulos son nullable=true
  // ════════════════════════════════════════════════════════════
  val geoSchema = StructType(Seq(
    StructField("clientId",   StringType,  nullable = true),
    StructField("cellId",     StringType,  nullable = false),
    StructField("latitude",   DoubleType,  nullable = false),
    StructField("longitude",  DoubleType,  nullable = false),
    StructField("signalDbm",  IntegerType, nullable = true),
    StructField("congestion", IntegerType, nullable = true),
    StructField("technology", StringType,  nullable = false),
    StructField("timestamp",  LongType,    nullable = false)
  ))

  // ════════════════════════════════════════════════════════════
  //  BLOQUE D — Leer desde Kafka
  // ════════════════════════════════════════════════════════════

  // TODO 4 ─────────────────────────────────────────────────
  // Conecta Spark con Kafka usando spark.readStream.
  // Topic: "geo-events"  |  Bootstrap server: "localhost:9092"
  // startingOffsets: "latest"  |  failOnDataLoss: "false"
  //
  // Después de .load(), parsea el JSON:
  //   1. selectExpr("CAST(value AS STRING) as json_str")
  //   2. select(from_json($"json_str", geoSchema).as("e"))
  //   3. select("e.*")
  //   4. withColumn("eventTime", (col("timestamp") / 1000).cast(TimestampType))
  // ──────────────────────────────────────────────────────────
/*
  val rawStream = spark.???
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", ???)
    .option("startingOffsets", ???)
    .option("maxOffsetsPerTrigger", ???)
    .load()



  val geoEvents = rawStream
    .selectExpr("CAST(value AS STRING) as json_str")     // ← rellenar
    .select(from_json($"json_str", geoSchema).as("e"))
    .select("e.*")
    .withColumn("eventTime", (col("timestamp") / 1000).cast(TimestampType))
    .as[GeoEvent]


  // ════════════════════════════════════════════════════════════
  //  BLOQUE E — ① DATA ENGINEERING: Limpieza de datos
  // ════════════════════════════════════════════════════════════
  // IMPORTANTE: el watermark se define UNA SOLA VEZ aquí.
  // Spark 3.5 no permite redefinirlo más adelante en el plan.

  // TODO 5 ─────────────────────────────────────────────────
  // Aplica tres filtros sobre geoEvents:
  //   - clientId no nulo Y no vacío
  //   - signalDbm no nulo
  //   - congestion entre 0 y 100
  // ──────────────────────────────────────────────────────────
  val cleanedStream = ???


  // TODO 6 ─────────────────────────────────────────────────
  // Aplica deduplicación sobre cleanedStream.
  // Elimina eventos del mismo clientId + cellId en el mismo segundo.
  // Usa: .dropDuplicates("campo1", "campo2", "campo3")
  // ──────────────────────────────────────────────────────────
  val dedupedStream = cleanedStream
    .dropDuplicates(???)  // TODO: campos para deduplicar

  // ════════════════════════════════════════════════════════════
  //  BLOQUE F — ② DATA SCIENCE: Ventana temporal y agregaciones
  // ════════════════════════════════════════════════════════════

  // TODO 7 ─────────────────────────────────────────────────
  // Agrupa por ventana de 2 minutos (deslizante cada 30s),
  // clientId y cellId. Calcula:
  //   - avg("congestion")  → avgCongestion
  //   - avg("signalDbm")   → avgSignalDbm
  //   - last("technology") → technology
  //   - count("*")         → eventCount
  //
  // Pista: window(col("eventTime"), "2 minutes", "30 seconds")
  // ATENCIÓN: usa approx_count_distinct, NO countDistinct
  //           (countDistinct no está soportado en streaming)
  // ──────────────────────────────────────────────────────────
  val aggregated = ???


  // ════════════════════════════════════════════════════════════
  //  BLOQUE G — Joins con tablas estáticas — YA DADO
  //  Observa el uso de broadcast() para optimizar el join
  // ════════════════════════════════════════════════════════════
  // broadcast() indica a Spark que envíe la tabla pequeña
  // a todos los workers en lugar de hacer shuffle de datos.
  val withConsents = ???

  val withCells = ???

  // TODO 8 ─────────────────────────────────────────────────
  // Filtra los candidatos: solo los que superen el umbral
  // de congestión específico de su celda.
  // La columna del stream es "avgCongestion"
  // La columna de cellsDF es "congestionThreshold"
  // ──────────────────────────────────────────────────────────
  val candidates = withCells
    .filter(???)  // TODO: avgCongestion >= congestionThreshold

  // ════════════════════════════════════════════════════════════
  //  BLOQUE H — ③ VISUALIZACIÓN: foreachBatch — YA DADO
  //  Observa cómo se llama a la IA y se escribe en múltiples sinks
  // ════════════════════════════════════════════════════════════
  val query = candidates.writeStream
    .outputMode(OutputMode.Append())
    .trigger(Trigger.ProcessingTime(15, TimeUnit.SECONDS))
    .option("checkpointLocation", "/tmp/geo-checkpoint")
    .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
      if (!batchDF.isEmpty) {
        println(s"\n[Batch $batchId] ${batchDF.count()} candidatos...")

        val alerts = batchDF.collect().map { row =>
          val decision = CampaignDecisionAI.decide(
            clientId      = row.getAs[String]("clientId"),
            cellId        = row.getAs[String]("cellId"),
            city          = row.getAs[String]("city"),
            congestion    = row.getAs[Int]("avgCongestion"),
            technology    = row.getAs[String]("technology"),
            segment       = row.getAs[String]("segment"),
            campaignsSent = row.getAs[Int]("campaignsSent"),
            signalDbm     = row.getAs[Int]("avgSignalDbm")
          )
          val alert = CampaignAlert(
            clientId    = row.getAs[String]("clientId"),
            cellId      = row.getAs[String]("cellId"),
            city        = row.getAs[String]("city"),
            congestion  = row.getAs[Int]("avgCongestion"),
            technology  = row.getAs[String]("technology"),
            segment     = row.getAs[String]("segment"),
            aiDecision  = decision.decision,
            aiReasoning = decision.reasoning,
            smsText     = decision.smsText,
            timestamp   = Instant.now().toString
          )
          printAlert(alert)
          alert
        }

        // Sink 1: CSV para el dashboard
        alerts.toSeq.toDF()
          .coalesce(1)
          .write.mode("append")
          .option("header", "true")
          .csv("/tmp/geo-campaigns/dashboard-feed")

        // Sink 2: Parquet solo para los SEND
        val sent = alerts.filter(_.aiDecision == "SEND")
        if (sent.nonEmpty)
          sent.toSeq.toDF().write.mode("append")
            .parquet(s"/tmp/geo-campaigns/sent/batch=$batchId")

        println(s"[Batch $batchId] 📤 SEND: ${sent.length} | ⏭️ SKIP: ${alerts.length - sent.length}")
      }
    }
    .start()

  query.awaitTermination()

  def printAlert(a: CampaignAlert): Unit = {
    val icon = if (a.aiDecision == "SEND") "📤" else "⏭️ "
    println(s"  $icon ${a.aiDecision} | ${a.segment.padTo(8,' ')} | ${a.cellId} (${a.city}) | ${a.congestion}%")
    println(s"     🤖 ${a.aiReasoning.take(80)}")
    if (a.aiDecision == "SEND") println(s"     📱 ${a.smsText.take(80)}")
  }
  */

}
