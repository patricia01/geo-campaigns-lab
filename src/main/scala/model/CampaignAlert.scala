package model

/**
 * Alerta de campaña generada por el pipeline.
 * Es el resultado final: lo que guardamos en Parquet y CSV.
 */
case class CampaignAlert(
                          clientId    : String,
                          cellId      : String,
                          city        : String,
                          congestion  : Int,
                          technology  : String,
                          segment     : String,
                          aiDecision  : String,   // "SEND" o "SKIP"
                          aiReasoning : String,   // Justificación de la IA
                          smsText     : String,   // Texto del SMS (si aiDecision == "SEND")
                          timestamp   : String
                        )