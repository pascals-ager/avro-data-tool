package io.pascals.avro.schema.utils

import org.apache.avro.{Schema, SchemaBuilder}
import io.pascals.avro.schema.utils.AvroDiffUtils.getNonNullSchema
import org.kitesdk.data.spi.SchemaUtil

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object AvroSchemaTransform {

  def transformToBISchema(
      schema: Schema,
      recordToFlatten: Option[String]
  ): Option[Schema] = {

    val partitionedSchema: Schema = SchemaBuilder
      .record(schema.getName)
      .namespace(schema.getNamespace)
      .fields()
      .name("year")
      .`type`()
      .intType()
      .noDefault()
      .name("month")
      .`type`()
      .intType()
      .noDefault()
      .name("day")
      .`type`()
      .intType()
      .noDefault()
      .endRecord()

    recordToFlatten match {
      case Some(recordName) =>
        val flattenedSchema = recursiveFlatten(schema, Seq(recordName), None)
        val flattened: mutable.Seq[Schema] = flattenedSchema.map { f =>
          SchemaBuilder
            .record(schema.getName)
            .namespace(schema.getNamespace)
            .fields()
            .name(f._1.reduceLeft { (left, right) =>
              right + "_" + left
            })
            .`type`(f._2.schema())
            .noDefault()
            .endRecord()
        }
        Option(SchemaUtil.merge((flattened :+ partitionedSchema).asJava))
      case None =>
        Option(SchemaUtil.merge(Seq(schema, partitionedSchema).asJava))
    }
  }

  def recursiveFlatten(
      schema: Schema,
      recordNames: Seq[String],
      parent: Option[String]
  ): ListBuffer[(Seq[String], Schema.Field)] = {
    val nonNullSchema = getNonNullSchema(schema)
    nonNullSchema.getType match {
      case Schema.Type.RECORD =>
        val record: ListBuffer[Schema.Field] =
          nonNullSchema.getFields.asScala.to[ListBuffer]
        record.flatMap { f: Schema.Field =>
          val actualSchema: Schema = getNonNullSchema(f.schema())
          if (actualSchema.getName == recordNames.head) {
            val allSchemaFields: Set[Schema.Field] =
              actualSchema.getFields.asScala.toSet
            val recordFields: mutable.Set[Schema.Field] =
              mutable.Set.empty[Schema.Field]

            allSchemaFields.foreach { fl =>
              getNonNullSchema(fl.schema()).getType match {
                case Schema.Type.RECORD => {
                  recordFields.add(fl)
                }
                case _ =>
              }
            }

            allSchemaFields
              .filterNot(recordFields)
              .to[ListBuffer]
              .map(fl => Tuple2(Seq(fl.name(), actualSchema.getName), fl)) ++ recordFields
              .to[ListBuffer]
              .flatMap { fl =>
                val rec = getNonNullSchema(fl.schema())
                recursiveFlatten(
                  rec,
                  rec.getName +: recordNames,
                  Some(rec.getName)
                )
              }
          } else {
            parent match {
              case Some(_) => mutable.ListBuffer((f.name() +: recordNames, f))
              case None    => mutable.ListBuffer((Seq(f.name()), f))
            }
          }
        }
      case _ =>
        ListBuffer(Tuple2(Seq(""), nonNullSchema.getFields.asScala.head))
    }
  }
}
