package io.github.pascals.avro.schema.utils

import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.scalatest.{FunSuite, Matchers}

class AvroSchemaTransformTest extends FunSuite with Matchers {

  val parserOne   = new Parser
  val parserTwo   = new Parser
  val parserThree = new Parser
  val testSchema: Schema = parserOne.parse(
    this.getClass
      .getResourceAsStream("/old/test_acquisition_traffic_scoring_nested.avsc")
  )
  val flattenedSchema: Schema = parserTwo.parse(
    this.getClass.getResourceAsStream(
      "/transformed/test_acquisition_traffic_scoring_flattened.avsc"
    )
  )
  val partitionedSchema: Schema = parserThree.parse(
    this.getClass.getResourceAsStream(
      "/transformed/test_acquisition_traffic_scoring_partitioned.avsc"
    )
  )

  test("Avro Schema Transform Test") {
    val trans: Option[Schema] = for {
      schema <- Option(testSchema)
      transformed <- AvroSchemaTransform
        .transformToBISchema(schema, Some("event_data"))
    } yield transformed
    trans shouldEqual Some(flattenedSchema)
  }

  test("Avro Schema Transform without flatten Test") {
    val trans: Option[Schema] = for {
      schema      <- Option(testSchema)
      transformed <- AvroSchemaTransform.transformToBISchema(schema, None)
    } yield transformed
    trans shouldEqual Some(partitionedSchema)
  }
}
