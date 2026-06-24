package model

// =============================================================
//  MODELOS DE DATOS — YA COMPLETADO
//  Lee este fichero con atención antes de empezar la práctica.
//  Las case classes definen el contrato de datos del pipeline.
// =============================================================

/**
 * Registro de consentimiento del cliente.
 * Se carga desde Parquet como tabla estática (no llega en streaming).
 */
case class ConsentRecord(
                          clientId      : String,
                          hasConsent    : Boolean,  // ¿Ha dado consentimiento de geolocalización?
                          segment       : String,   // PREMIUM | STANDARD | BASIC
                          targetCells   : String,   // Celdas objetivo separadas por coma
                          campaignsSent : Int       // Campañas enviadas este mes
                        )
