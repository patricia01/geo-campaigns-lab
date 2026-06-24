# 📡 Práctica: Campañas 5G en Tiempo Real
## Máster en Ingeniería de Datos — EOI
### Basado en hechos reales :)

---

## Objetivo

Construir un pipeline de Spark Structured Streaming que, en tiempo real:

1. **Ingiere** eventos de geolocalización desde Kafka
2. **Limpia** datos malformados y duplicados (rol Data Engineering)
3. **Detecta** clientes en celdas saturadas con consentimiento activo (rol Data Science)
4. **Decide** si enviar una campaña SMS mediante IA (Claude API)
5. **Escribe** los resultados en CSV y Parquet para visualización

Al final de la práctica tendrás un pipeline funcionando end-to-end que dispara alertas reales cuando el productor simula un evento masivo en Madrid.

---

## Estructura del proyecto

```
geo-campaigns-lab/
├── pom.xml
├── docker/
│   └── docker-compose.yml          ← Kafka local
└── src/main/scala/
    ├── model/
    │   └── Models.scala            ← ✅ YA DADO — lee antes de empezar
    ├── setup/
    │   └── DataSetupJob.scala      ← ⚠️  TODO 1, TODO 2
    ├── producer/
    │   └── GeoEventProducer.scala  ← ✅ YA DADO — no tocar
    ├── ai/
    │   └── CampaignDecisionAI.scala ← ⚠️  TODO 3
    └── streaming/
        └── GeoCampaignPipeline.scala ← ⚠️  TODO 4-8 (núcleo de la práctica)
```

**Leyenda:**
- ✅ YA DADO → leer y entender, no modificar
- ⚠️ TODO → completar por el alumno

---

## Requisitos previos

| Herramienta | Versión | Verificar |
|---|---|---|
| Java JDK | 17 | `java -version` |
| Maven | 3.8+ | `mvn -version` |
| Docker Desktop | cualquiera | Abierto y corriendo |
| IntelliJ IDEA | 2023+ | Plugin Scala instalado |

**Antes de empezar:** abre el proyecto en IntelliJ (`File → Open`) y espera a que Maven descargue las dependencias (~3 min la primera vez).

---

## PASO 0 — Leer los modelos de datos

**Fichero:** `model/Models.scala`

Antes de tocar ningún TODO, lee este fichero completo. Define las cuatro case classes del pipeline:

- `GeoEvent` → lo que llega de Kafka
- `ConsentRecord` → tabla de consentimientos (Parquet estático)
- `CellConfig` → configuración de celdas (Parquet estático)
- `CampaignAlert` → resultado del pipeline

> 💡 **¿Por qué case class y no class normal?**
> Las case classes en Scala son inmutables, tienen igualdad por valor y permiten acceso directo a los campos. Spark las usa para crear el schema del Dataset automáticamente con `.as[CaseClass]`.

### Preguntas conceptuales 1


> **P1.1** — ¿Qué diferencia hay entre `ConsentRecord` (que se carga desde Parquet) y `GeoEvent` (que llega de Kafka)? ¿Por qué una es "estática" y la otra es "streaming"?


---

## PASO 1 — Levantar Kafka

Abre una terminal y ejecuta:

```bash
cd docker/
docker-compose up -d
```

Espera ~20 segundos. Verifica que está corriendo:

```bash
docker-compose ps
# Los tres servicios deben aparecer como "healthy"
```

Abre el navegador en **http://localhost:8080** — deberías ver la Kafka UI.

---

## PASO 2 — Generar los datos de referencia

**Fichero a completar:** `setup/DataSetupJob.scala`

Este job genera los Parquet que el pipeline leerá como tablas estáticas.

### TODO 1 — Guardar los consentimientos

Localiza el comentario `TODO 1` en `DataSetupJob.scala`. Tienes el DataFrame `consents` creado. Guárdalo en Parquet.

### TODO 2 — Guardar las celdas

Localiza el comentario `TODO 2`. Haz lo mismo con el DataFrame `cells`.


Cuando hayas completado ambos TODOs, ejecuta:

```bash
mvn compile exec:exec -Dexec.mainClass="setup.DataSetupJob"
```

Deberías ver una muestra de los datos en consola.

### Preguntas conceptuales 2

> **P2.1** — ¿Por qué usamos `mode("overwrite")` y no `mode("append")`? ¿Qué ocurriría si ejecutaras el job dos veces con `append`?

> **P2.2** — En producción real, ¿de dónde vendrían estos datos de consentimiento? ¿Con qué frecuencia habría que actualizarlos?

> **P2.3** — Spark escribe los Parquet particionados en múltiples ficheros. ¿Por qué crees que hace esto en lugar de escribir un único fichero?

---

## PASO 3 — Observar el productor

**Fichero:** `producer/GeoEventProducer.scala` *(no modificar)*

Lee el fichero y entiende:
- Cómo se conecta a Kafka (`KafkaProducer`)
- Cómo serializa la `GeoEvent` a JSON con Jackson
- Cómo simula el evento masivo a los 60 segundos
- Cómo inyecta eventos sucios (~5% con campos nulos)

Arranca el productor en una **nueva terminal**:

```bash
mvn exec:exec -Dexec.mainClass="producer.GeoEventProducer"
```

Observa la consola:
- 🟢 congestión baja
- 🟡 congestión media
- 🔴 congestión alta (después del evento masivo)
- 💀 evento sucio

Verifica que los mensajes llegan a Kafka en **http://localhost:8080** → Topics → geo-events.

### Preguntas conceptuales 3

> **P3.1** — El productor serializa con Jackson a JSON y el pipeline parsea con `from_json`. ¿Por qué no se usa un formato binario como Avro? ¿Cuándo usarías esos formatos en producción?

> **P3.2** — El productor tiene `acks = "1"`. ¿Qué significa esto? ¿Cuál sería la diferencia con `acks = "all"`?

> **P3.3** — El productor inyecta un 5% de eventos con `clientId = null`. ¿Qué pasaría en el pipeline si no filtráramos estos eventos antes del join con la tabla de consentimientos?

---

## PASO 4 — Completar el motor de IA (opcional con API key)

**Fichero a completar:** `ai/CampaignDecisionAI.scala`

### TODO 3 — Lógica de decisión

Localiza el `TODO 3` en el método `decide`. Tienes que completar la lógica:

```scala
// Si no hay API key → usar offlineDecision
// Si hay API key → llamar a callClaudeAPI y manejar el resultado


```

> 💡 Si no tienes API key, el modo offline ya funciona con reglas de negocio locales. El pipeline funciona igual.

Para configurar la API key (opcional):
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

### Preguntas conceptuales 4

> **P4.1** — El prompt incluye las reglas de negocio (PREMIUM > 60%, STANDARD > 70%...). ¿Cuál es la ventaja de que la IA aplique estas reglas frente a hacerlo directamente en código Scala?

> **P4.2** — La IA puede decidir `SKIP` aunque técnicamente se cumplan los umbrales. ¿Se te ocurre algún caso de negocio real donde esto sea deseable?

---

## PASO 5 — Construir el pipeline principal

**Fichero a completar:** `streaming/GeoCampaignPipeline.scala`

Este es el núcleo de la práctica. Sigue los TODOs en orden.

---

### TODO 4 — Cargar Parquet, Conectar con Kafka y parsear JSON

Cargar los parquet de consentimientos y celdas.

```scala
val consentsDF = ???
val cellsDF    = ???
```
### Preguntas conceptuales 

> **P4.1** — ¿Tiene sentido incluir un .cache aquí? 
> 
> **P4.2** — ¿Qué ocurriría en el caso de que se actualizaran uno de los dos DF anteriores en el disco?


Conectarse a Kafka:

```scala
// Leer desde el principio del topic con 1000 eventos por batch cada 3 segundos

val rawStream = spark.readStream
  ...

val geoEvents = rawStream
  .selectExpr("CAST(value AS STRING) as json_str")     // ← rellenar
  .select(from_json($"json_str", geoSchema).as("e"))
  .select("e.*")
  .withColumn("eventTime", (col("timestamp") / 1000).cast(TimestampType))
```
> **P4.3** — ¿Qué sentido tiene poner un maxOffsetsPerTrigger?¿Qué ocurre si lo quito?



> 💡 El valor de Kafka siempre llega como `Array[Byte]`. Hay que castearlo a String antes de parsear el JSON.

---

### TODO 5 — Limpieza de datos (Data Engineering)
Aplica tres filtros sobre geoEvents y además cambia la case class de GeoEvent para los campos nulos:

- clientId no nulo Y no vacío

- signalDbm no nulo

- congestion entre 0 y 100

```scala
val cleanedStream = ???
```
---

### TODO 6 — Deduplicación
Eliminar los duplicados , entiendo por duplicados un registro que tenga:

- clientId,cellId,eventTime

```scala

val dedupedStream = ???
```

> **P5.1** — ¿Esto es una función stateful o stateless?
>
> **P5.2** — ¿Tiene sentido incluir una watermark aquí?¿Y en el cleanedStream?

---

### TODO 7 — Ventana temporal y agregaciones

> ⚠️ **MUY IMPORTANTE:** El watermark se define **una sola vez** aquí. Si lo vuelves a definir más adelante en el plan, Spark 3.5 lanzará un error: `Redefining watermark is disallowed`.

```scala
// EJERCICIO: Agrupación temporal (Ventanas deslizantes)
// 
// Objetivo: Calcular el comportamiento medio de la red en bloques de tiempo.
//
// 1. Aplica un 'groupBy' usando una ventana de tiempo ('window') sobre "eventTime".
//    - Duración de la ventana: "2 minutes".
//    - Actualización (deslizamiento): "30 seconds".
// 2. Añade también al 'groupBy' las columnas "clientId" y "cellId".
// 3. Calcula las siguientes agregaciones ('agg'):
//    - avg("congestion")  -> renómbralo a "avgCongestion"
//    - avg("signalDbm")   -> renómbralo a "avgSignalDbm"
//    - last("technology") -> renómbralo a "technology"
//    - count("*")         -> renómbralo a "eventCount"
// 4. Usa 'withColumn' para convertir "avgCongestion" y "avgSignalDbm" a enteros (IntegerType).

val aggregated = ???
  // Escribe tu código aquí...
```

> ⚠️ Usa siempre `approx_count_distinct` en lugar de `countDistinct` en streaming. `countDistinct` no está soportado y lanzará un error en runtime.

---

### TODO 8 — Join y Filtrar por umbral de congestión

```scala
// Vamos a cruzar con los consentimientos del cliente y solo filtramos los que hayan dado el consentimiento
val withConsent = ???

//Lo mismo pero con el diccionario de celdas
val withCells = ???

// Solo son objetivo aquellas personas para las que su media de congestión sea superior al umbral definido
val candidates = withCells
  
```

```scala
val candidates = withCells
  .filter(col("avgCongestion") >= col("congestionThreshold"))
```

> 💡 Fíjate en que `congestionThreshold` viene de la tabla `cellsDF` (Parquet) y `avgCongestion` viene del stream. El join los ha unido en el mismo DataFrame.

---

### Ejecutar el pipeline

Con el productor corriendo en otra terminal:

```bash
mvn compile exec:exec -Dexec.mainClass="streaming.GeoCampaignPipeline"
```

Espera 2-3 minutos. Las primeras alertas aparecerán cuando Spark cierre la primera ventana temporal. A los 60 segundos el productor activa el evento masivo y verás llegar alertas con SMS.

### Preguntas conceptuales 5

> **P5.1** — La ventana es de 2 minutos con slide de 30 segundos. ¿Cuántas ventanas activas hay simultáneamente en un momento dado? ¿Qué pasaría si pusieras slide = duración?

> **P5.2** — ¿Por qué usamos `OutputMode.Append()` y no `OutputMode.Complete()`? ¿En qué situación usarías `Complete`?

> **P5.3** — El join con `consentsDF` usa `broadcast()`. ¿Qué es un broadcast join? ¿Por qué es importante en streaming? ¿Qué pasaría si no lo usaras?

> **P5.4** — El `checkpointLocation` es obligatorio en producción. ¿Para qué sirve? ¿Qué ocurriría si el pipeline se cae y reinicia sin checkpoint?

---

## PASO 6 — Verificar resultados

En el CampaignDecisionAI usa siempre modo offline.

### Comprobar alertas en consola
Deberías ver algo así:
```
[Batch 3] 2 candidatos...
  📤 SEND | PREMIUM  | CELL_003 (Madrid Retiro) | 87%
     🤖 Segmento PREMIUM, congestión 87%, 1 campaña enviada.
     📱 Tu red está saturada. Activa la priorización 5G. Responde SÍ (gratis 24h).
  ⏭️  SKIP | BASIC    | CELL_007 (Madrid Bernabéu) | 74%
     🤖 Congestión 74% no supera umbral para BASIC
     
```

### Preguntas conceptuales 6

> **P6.1** — Busca cuellos de botella en el almacenamiento de datos en el CSV/Parquet (analogía del escritorio)


### Comprobar Parquet generado
```bash
ls /tmp/geo-campaigns/sent/
ls /tmp/geo-campaigns/dashboard-feed/
```

### Leer los resultados con Spark
En otra terminal con spark-shell o añade este código:
```scala
val results = spark.read.parquet("/tmp/geo-campaigns/sent/**")
results.show(10, truncate = false)
results.groupBy("segment", "aiDecision").count().show()
```

---

## PASO 7 — Reto opcional

Si has terminado antes de tiempo, elige uno o más retos:

### Reto A — Nueva señal de filtrado
Añade una regla adicional en `GeoCampaignPipeline.scala`: no enviar campaña si el cliente lleva menos de 3 eventos en la ventana (no tiene suficiente datos para confirmar que está saturado).

```scala
// Pista: añade un filter después de aggregated
.filter(col("eventCount") >= 3)
```

### Reto B — Métrica de limpieza
Cuenta cuántos eventos se están descartando en la limpieza. Añade dentro del `foreachBatch` una comparación entre el número de filas antes y después de los filtros.

### Reto C — Segundo topic de Kafka
Publica las alertas SEND en un segundo topic de Kafka llamado `campaign-alerts`. Otro equipo puede consumirlo con un segundo job de Spark.

```scala
// Pista: dentro del foreachBatch, para cada alerta SEND:
val kafkaProducer = new KafkaProducer[String, String](props)
kafkaProducer.send(new ProducerRecord("campaign-alerts", alert.clientId, json))
```

---

## Soluciones de referencia

Si te quedas atascado, los TODOs tienen estas soluciones:


---

## Troubleshooting

| Error | Causa | Solución |
|-------|-------|----------|
| `Path not found: /tmp/geo-campaigns/consents` | DataSetupJob no ejecutado | Ejecutar Paso 2 primero |
| `UnknownTopicOrPartitionException` | Pipeline arrancó antes que el productor | Arrancar productor primero |
| `Redefining watermark is disallowed` | withWatermark llamado dos veces | Eliminar el segundo withWatermark |
| `countDistinct not supported` | Usar countDistinct en streaming | Cambiar a approx_count_distinct |
| Error de checkpoint | Estado inconsistente | `rm -rf /tmp/geo-checkpoint` |
| No aparecen alertas | Ventana no cerrada aún | Esperar 2-3 minutos |
| `getSubject is not supported` | Java 26 con Maven | `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` |

---

## Resumen de comandos

```bash
# 1. Kafka
cd docker && docker-compose up -d

# 2. Datos de referencia (una vez)
mvn compile exec:exec -Dexec.mainClass="setup.DataSetupJob"

# 3. Productor (terminal nueva, dejar corriendo)
mvn exec:exec -Dexec.mainClass="producer.GeoEventProducer"

# 4. Pipeline principal (terminal nueva)
mvn compile exec:exec -Dexec.mainClass="streaming.GeoCampaignPipeline"

# 5. Ver resultados
ls /tmp/geo-campaigns/sent/
```
