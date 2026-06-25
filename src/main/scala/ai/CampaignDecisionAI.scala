package ai

import scala.util.{Failure, Success, Try}

/**
 * =============================================================
 *  PASO 3 — MOTOR DE DECISIÓN IA
 *
 *  Este objeto llama a Claude API para decidir si enviar
 *  o no una campaña SMS a un cliente.
 *
 *  Sin API key funciona en modo offline (offlineDecision).
 *  Configura: export ANTHROPIC_API_KEY="sk-ant-..."
 * =============================================================
 */
/*
El objetivo del objeto CampaignDecisionAI es decidir si se le envía un SMS a un cliente de telefonía para venderle un "bono de priorización 5G",
basándose en cómo de saturada está su antena y qué tipo de cliente es.
 */

object CampaignDecisionAI {

  private val API_KEY = sys.env.getOrElse("ANTHROPIC_API_KEY", "")
  private val API_URL = "https://api.anthropic.com/v1/messages"
  private val MODEL   = "claude-sonnet-4-20250514"

  // Clase que representa la decisión de la IA
  case class AIDecision(decision: String, reasoning: String, smsText: String)

  /**
   * Punto de entrada principal.
   * Decide si llamar a la API o usar el modo offline.
   */
  def decide(
    clientId      : String,
    cellId        : String,
    city          : String,
    congestion    : Int,
    technology    : String,
    segment       : String,
    campaignsSent : Int,
    signalDbm     : Int
  ): AIDecision = {

    // TODO 3 ───────────────────────────────────────────────
    // Completa la condición: si API_KEY está vacía, usar modo
    // offline. Si no, llamar a la API y manejar el resultado.
    //
    // Pista: usa un if/else con API_KEY.isEmpty
    //        Para la llamada a la API usa callClaudeAPI(...)
    //        El resultado es un Try[AIDecision]:
    //          Success(d) → devolver d
    //          Failure(ex) → logar el error y usar offlineDecision
    // ──────────────────────────────────────────────────────

offlineDecision(segment,congestion,technology,campaignsSent)

  }

  // ── Llamada HTTP a Claude API — YA DADO ──────────────────
  // No es necesario modificar este método.
  private def callClaudeAPI(
    clientId: String, cellId: String, city: String,
    congestion: Int, technology: String, segment: String,
    campaignsSent: Int, signalDbm: Int
  ): Try[AIDecision] = Try {

    val prompt =
      s"""Eres el motor de decisión de campañas de marketing de una telco española.
         |Decide si enviar un SMS al cliente ofreciéndole priorización de tráfico 5G.
         |
         |CONTEXTO:
         |• Cliente: $clientId | Segmento: $segment
         |• Celda: $cellId ($city) | Saturación: $congestion% | Red: $technology
         |• Señal: ${signalDbm} dBm | Campañas enviadas este mes: $campaignsSent
         |
         |REGLAS:
         |• PREMIUM: enviar si congestión > 60% y campañas < 5
         |• STANDARD: enviar si congestión > 70% y campañas < 3
         |• BASIC: enviar si congestión > 80% y campañas < 2
         |• No enviar si señal peor de -95 dBm
         |• SMS máximo 160 caracteres, en español
         |
         |Responde SOLO con este JSON (sin markdown):
         |{"decision":"SEND","reasoning":"motivo","smsText":"texto del SMS"}
         |Si no envías: {"decision":"SKIP","reasoning":"motivo","smsText":""}""".stripMargin

    val client = java.net.http.HttpClient.newHttpClient()
    val body   = s"""{"model":"$MODEL","max_tokens":400,"messages":[{"role":"user","content":${escapeJson(prompt)}}]}"""

    val request = java.net.http.HttpRequest.newBuilder()
      .uri(java.net.URI.create(API_URL))
      .header("Content-Type",      "application/json")
      .header("x-api-key",         API_KEY)
      .header("anthropic-version", "2023-06-01")
      .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body))
      .build()

    val response = client.send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
    if (response.statusCode() != 200)
      throw new RuntimeException(s"HTTP ${response.statusCode()}: ${response.body().take(200)}")

    parseResponse(extractText(response.body()), segment, congestion)
  }

  private def parseResponse(text: String, segment: String, congestion: Int): AIDecision = {
    try {
      val clean = text.trim.replaceAll("(?s)```json|```", "").trim
      AIDecision(
        decision  = extractField(clean, "decision"),
        reasoning = extractField(clean, "reasoning"),
        smsText   = extractField(clean, "smsText")
      )
    } catch { case _: Exception => offlineDecision(segment, congestion, "5G", 0) }
  }

  private def extractField(json: String, field: String): String = {
    val p = s""""$field"\\s*:\\s*"((?:[^"\\\\]|\\\\.)*)"""".r
    p.findFirstMatchIn(json).map(_.group(1).replace("\\n"," ").replace("\\\"","\"")).getOrElse("")
  }

  private def extractText(json: String): String = {
    val p = """"text"\s*:\s*"((?:[^"\\]|\\.)*)"""".r
    p.findFirstMatchIn(json).map(_.group(1).replace("\\n","\n")).getOrElse("")
  }

  private def escapeJson(s: String): String =
    "\"" + s.replace("\\","\\\\").replace("\"","\\\"").replace("\n","\\n") + "\""

  // ── Modo offline — YA DADO ───────────────────────────────
  // Se usa cuando no hay API key configurada.
  def offlineDecision(
    segment: String, congestion: Int,
    technology: String, campaignsSent: Int
  ): AIDecision = {
    val send = segment match {
      case "PREMIUM"  => congestion > 60 && campaignsSent < 5
      case "STANDARD" => congestion > 70 && campaignsSent < 3
      case "BASIC"    => congestion > 80 && campaignsSent < 2
      case _          => false
    }
    if (send) {
      val sms = if (technology == "5G")
        "Tu red está saturada. Activa la priorización 5G. Responde SÍ (gratis 24h)."
      else
        "Zona congestionada. Activa 5G prioritario y navega sin cortes. Responde SÍ (gratis 24h)."
      AIDecision("SEND", s"Segmento $segment, congestión $congestion%, $campaignsSent campañas.", sms)
    } else {
      val reason = if (campaignsSent >= 3) s"Límite de campañas alcanzado ($campaignsSent/mes)"
                   else s"Congestión $congestion% no supera umbral para $segment"
      AIDecision("SKIP", reason, "")
    }
  }
}
