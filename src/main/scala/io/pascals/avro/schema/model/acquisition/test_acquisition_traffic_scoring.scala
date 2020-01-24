/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package io.pascals.avro.schema.model.acquisition

case class test_acquisition_traffic_scoring(
    `type`: String,
    id: String,
    happened: String,
    processed: String,
    tracking_id: String,
    referenced_event_id: Option[String],
    source_attributes: source_attributes,
    event_data_score: Int,
    event_data_decision: String,
    event_data_reason: Option[String]
)
