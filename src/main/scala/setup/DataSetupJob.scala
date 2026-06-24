package setup

import org.apache.spark.sql.SparkSession

/**
 * =============================================================
 *  PASO 1 — GENERADOR DE DATOS DE REFERENCIA
 *  Ejecutar UNA SOLA VEZ antes del pipeline.
 *
 *  mvn compile exec:exec -Dexec.mainClass="setup.DataSetupJob"
 * =============================================================
 *
 *  Este job crea dos ficheros Parquet en /tmp/geo-campaigns/:
 *    - consents/  → tabla de consentimientos de clientes
 *    - cells/     → tabla de celdas objetivo para campañas
 *
 *  En producción real estos datos vendrían de un sistema CRM
 *  y de la configuración de red. Aquí los simulamos.
 */
object DataSetupJob extends App {

  // ── SparkSession — YA DADO ───────────────────────────────
  val spark = SparkSession.builder()
    .appName("DataSetup")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  println("\n⚙️  Generando datos de referencia...\n")

  // ── Tabla de consentimientos ──────────────────────────────
  // YA DADO: la lógica de generación está completa.
  // PREGUNTA: ¿Qué ocurriría si hasConsent fuera siempre true?
  //           ¿Qué implicaciones legales tendría en producción?

  val segments = Seq("PREMIUM", "PREMIUM", "STANDARD", "STANDARD", "STANDARD", "BASIC")
  val allCells = (1 to 20).map(i => f"CELL_$i%03d")

  val consents = (1 to 50).map { i =>
    val seg    = segments(i % segments.length)
    val nCells = if (seg == "PREMIUM") 8 else if (seg == "STANDARD") 5 else 3
    val cells  = allCells.drop((i * 3) % (allCells.length - nCells)).take(nCells).mkString(",")
    (f"CLIENT_$i%04d", i % 10 != 0, seg, cells, i % 4)
  }.toDF("clientId", "hasConsent", "segment", "targetCells", "campaignsSent")

  // TODO 1 ─────────────────────────────────────────────────
  // Guarda el DataFrame `consents` en formato Parquet
  // en la ruta "/tmp/geo-campaigns/consents".
  // Se tiene que poder re-ejecutar sin errores.
  // ──────────────────────────────────────────────────────────


  println(s"✅ Consents: ${consents.count()} registros")

  // ── Tabla de celdas objetivo ──────────────────────────────
  val cities = Seq(
    ("Madrid","Centro"), ("Madrid","Bernabéu"), ("Madrid","Retiro"),
    ("Barcelona","Eixample"), ("Barcelona","Gràcia"),
    ("Valencia","Centro"), ("Sevilla","Centro"),
    ("Bilbao","Centro"), ("Málaga","Costa"), ("Alicante","Centro"),
    ("Madrid","Sur"), ("Madrid","Este"), ("Barcelona","Poblenou"),
    ("Valencia","Norte"), ("Sevilla","Norte"),
    ("Zaragoza","Centro"), ("Murcia","Centro"), ("Palma","Centro"),
    ("Las Palmas","Centro"), ("Tenerife","Sur")
  )

  val cells = allCells.zipWithIndex.map { case (cellId, i) =>
    val (city, zone) = cities(i % cities.length)
    val active       = i % 5 != 0
    val threshold    = 60 + (i % 3) * 10
    (cellId, active, threshold, city, zone)
  }.toDF("cellId", "campaignActive", "congestionThreshold", "city", "zone")

  // TODO 2 ─────────────────────────────────────────────────
  // Guarda el DataFrame `cells` en Parquet
  // en la ruta "/tmp/geo-campaigns/cells".
  // ──────────────────────────────────────────────────────────
  //

  println(s"✅ Cells: ${cells.count()} registros")

  println("\n📋 Muestra consentimientos:")
  consents.show(5, truncate = false)
  println("📋 Muestra celdas:")
  cells.show(5, truncate = false)

  spark.stop()
  println("✅ Setup completo.\n")
}
