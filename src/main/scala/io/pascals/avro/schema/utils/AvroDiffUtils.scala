package io.pascals.avro.schema.utils

import com.typesafe.scalalogging.Logger
import io.pascals.avro.schema.metadata._
import org.apache.avro.{Schema, SchemaBuilder}
import org.kitesdk.data.spi.SchemaUtil

import scala.collection.JavaConverters._
import scala.collection.mutable

object AvroDiffUtils {
  /*
   * Detect the diff between two version of a schema and generate an ALTER TABLE statement to modify the Hive table
   * Enforce rules for schema compatibility and ensure that changes are bound to the rules.
   * */
  private val log = Logger(getClass)

  def getNonNullSchema(schema: Schema): Schema = {
    schema.getType match {
      case Schema.Type.UNION => {
        val list_types = schema.getTypes
        var nullFound  = false
        var result     = schema
        list_types.forEach { typ =>
          if (!typ.getType.eq(Schema.Type.NULL)) {
            assert(!nullFound, "Cannot handle union of two non-null types")
            nullFound = true
            result = typ
          }
        }
        result
      }
      case _ => schema
    }
  }

  def typeMetaGenerator(schema: Schema, name: String): (String, TypeMeta) = {
    val scheme = getNonNullSchema(schema)
    scheme.getType match {
      case Schema.Type.RECORD =>
        val nonNullSchema             = getNonNullSchema(schema)
        val record: Seq[Schema.Field] = nonNullSchema.getFields.asScala
        val clsFields: Seq[ClassFieldMeta] = record.map(f => {
          val nonNullSchema = getNonNullSchema(f.schema())
          val typ           = typeMetaGenerator(nonNullSchema, f.name())
          ClassFieldMeta(typ._1, typ._1, typ._2, Seq())
        })
        (name, ClassTypeMeta(name, name, name, Seq(), clsFields))
      case Schema.Type.MAP =>
        val t: (String, TypeMeta) =
          typeMetaGenerator(scheme.getValueType, scheme.getValueType.getName)
        (name, MapTypeMeta(StringType, t._2))
      case Schema.Type.ARRAY =>
        val t: (String, TypeMeta) = typeMetaGenerator(
          scheme.getElementType,
          scheme.getElementType.getName
        )
        (name, SeqTypeMeta(t._2))
      case Schema.Type.FLOAT =>
        (name, FloatType)
      case Schema.Type.DOUBLE =>
        (name, DoubleType)
      case Schema.Type.LONG =>
        (name, LongType)
      case Schema.Type.INT =>
        (name, IntegerType)
      case Schema.Type.STRING =>
        (name, StringType)
      case Schema.Type.BOOLEAN =>
        (name, BooleanType)
      case Schema.Type.BYTES =>
        (name, ByteType)
      case _ =>
        log.info(s"$name did not match a supported type!")
        (name, NullType)
    }
  }

  def diffAvroSchemas(schemaOne: Schema, schemaTwo: Schema): Option[Schema] = {
    val fieldsOne: mutable.Seq[Schema.Field] = schemaOne.getFields.asScala
    val fieldsTwo: mutable.Seq[Schema.Field] = schemaTwo.getFields.asScala
    val diff: Set[Schema.Field]              = fieldsTwo.toSet.filterNot(fieldsOne.toSet)
    val schemaNamespace: String              = schemaTwo.getNamespace
    val schemaName: String                   = schemaTwo.getName

    val diffedSchemas: Set[Schema] = diff.map(f => {
      SchemaBuilder
        .record(schemaName)
        .namespace(schemaNamespace)
        .fields()
        .name(f.name())
        .`type`(f.schema())
        .noDefault()
        .endRecord()
    })
    Option(SchemaUtil.merge(diffedSchemas.asJava))
  }

}
