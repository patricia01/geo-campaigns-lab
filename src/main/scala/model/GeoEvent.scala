package model

/**
 * Evento de geolocalización que llega desde Kafka.
 * Cada vez que un cliente está en una celda, el productor
 * envía un mensaje con esta estructura en formato JSON.
 */
case class GeoEvent(
                     clientId   : String,   // ID del cliente (puede llegar nulo si dato sucio)
                     cellId     : String,   // ID de la antena
                     latitude   : Double,   // Latitud aproximada de la celda
                     longitude  : Double,   // Longitud aproximada
                     signalDbm  : Int,      // Potencia de señal en dBm (puede llegar nulo)
                     congestion : Int,      // % de saturación de la celda (0-100)
                     technology : String,   // Tecnología: "4G" o "5G"
                     timestamp  : Long      // Epoch millis UTC
                   )