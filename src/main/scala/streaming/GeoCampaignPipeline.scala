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




}
