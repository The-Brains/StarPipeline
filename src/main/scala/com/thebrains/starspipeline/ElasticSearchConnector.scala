package com.thebrains.starspipeline

import com.thebrains.elasticsearchcommon.{ElasticSearchClient, ElasticSearchWrapper}
import com.thebrains.starspipeline.model.Star
import com.sksamuel.elastic4s.mappings.FieldDefinition

class ElasticSearchConnector extends ElasticSearchWrapper[Star](
  es = new ElasticSearchClient(),
  indexName = "stars",
  documentType = "star"
) {

  override def getIndexMapping: Seq[FieldDefinition] = {
    Seq(
      keywordField("id"),
      doubleField("temperatureInKelvin"),
      doubleField("luminosityInSol"),
      doubleField("ageInYear"),
      doubleField("massInSol"),
      doubleField("probabilityOfCorrectness")
    )
  }
}
