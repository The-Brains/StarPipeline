package com.thebrains.starspipeline.model

import com.thebrains.elasticsearchcommon.ElasticSearchDocument

case class StarFromCsv(
  idCl: Option[String],
  idN: Option[String],
  idId: Option[String],
  bvMag: Option[String],
  vMag: Option[String],
  lc: Option[String],
  temperatureInKelvin: Option[String],
  luminosityInSol: Option[String],
  ageInYear: Option[String],
  massInSol: Option[String],
  probabilityOfRightValues: Option[String]
) {
  def toStar: Option[Star] = {
    try {
      Some(Star(
        idCl = idCl.map(_.trim.toInt),
        idN = idN.map(_.trim.toInt),
        idId = idId.map(_.trim.toInt),
        temperatureInKelvin = temperatureInKelvin.map(_.trim.toDouble),
        luminosityInSol = luminosityInSol.map(_.trim.toDouble),
        ageInYear = ageInYear.map(_.trim.toDouble),
        massInSol = massInSol.map(_.trim.toDouble),
        probabilityOfRightValues = probabilityOfRightValues.map(_.trim.toDouble)
      ))
    } catch {
      case _: java.lang.NumberFormatException => None
    }
  }
}

case class Star(
  idCl: Option[Int],
  idN: Option[Int],
  idId: Option[Int],
  temperatureInKelvin: Option[Double],
  luminosityInSol: Option[Double],
  ageInYear: Option[Double],
  massInSol: Option[Double],
  probabilityOfRightValues: Option[Double]
) extends ElasticSearchDocument {
  lazy val uniqueId: String = s"${idCl.getOrElse(0)}-${idN.getOrElse(0)}-${idId.getOrElse(0)}"

  override def toDocument: Map[String, Any] = {
    Map(
      "id" -> uniqueId,
      "temperatureInKelvin" -> temperatureInKelvin.orNull,
      "luminosityInSol" -> luminosityInSol.orNull,
      "ageInYear" -> ageInYear.orNull,
      "massInSol" -> massInSol.orNull,
      "probabilityOfCorrectness" -> probabilityOfRightValues.orNull
    )
  }

  override def getId: String = uniqueId
}