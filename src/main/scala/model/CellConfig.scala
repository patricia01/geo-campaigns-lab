package model


/**
 * Configuración de una celda para campañas.
 * También se carga desde Parquet como tabla estática.
 */
case class CellConfig(
                       cellId              : String,
                       campaignActive      : Boolean,  // ¿Está esta celda activa en campaña?
                       congestionThreshold : Int,      // % mínimo de saturación para lanzar campaña
                       city                : String,
                       zone                : String
                     )