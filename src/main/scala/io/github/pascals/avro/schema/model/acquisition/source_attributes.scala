/** MACHINE-GENERATED FROM AVRO SCHEMA. DO NOT EDIT DIRECTLY */
package io.github.pascals.avro.schema.model.acquisition

case class source_attributes(
    id: String,
    origin: String,
    internal_data: Option[Map[String, Option[String]]],
    external_data: Option[Map[String, Option[String]]]
)
