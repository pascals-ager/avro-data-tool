package io.github.pascals.avro.schema

package object metadata {

  /**
    *
    * @param name  of an annotation attribute
    * @param value value of an annotation attribute
    *
    *              Example @hiveTable(tableName="Test")
    *              hiveTable is the annotation
    *              tableName is the attribute name with value "Test"
    */
  case class AnnotationAttribute(name: String, value: String)

  trait HasValueAttribute {
    val attributes: Seq[AnnotationAttribute]

    def getValue: String = {
      attributes.head.value
    }
  }

  /**
    *
    * @param name       of an annotation
    * @param attributes is the Seq of annotation attributes
    *                   Example @hiveTable(tableName="Test", tableType="External")
    *                   hiveTable is the annotation name
    *                   Seq(AnnotationAttribute("tableName", "Test"), AnnotationAttribute("tableType", "External")) are the attributes
    */
  case class AnnotationMeta(name: String, attributes: Seq[AnnotationAttribute])
      extends HasValueAttribute

  trait HasAnnotations {
    val annotations: Iterable[AnnotationMeta]

    def getAnnotationValue(annotationName: String): Option[String] = {
      annotations.find(_.name == annotationName).map(_.getValue)
    }

    def annotationExists(annotationName: String): Boolean = {
      val annotation: Option[AnnotationMeta] =
        annotations.find(_.name == annotationName)
      annotation.isDefined
    }

    def getAnnotationsByName(annotationName: String): Iterable[AnnotationMeta] =
      annotations.filter(_.name == annotationName)
  }

  sealed trait TypeMeta {
    val packageName: String
    val typeName: String
  }

  /*
   * Define primitive types that the Metadata extractor understands.
   *
   * */
  sealed abstract class PrimitiveTypeMeta(
      val packageName: String,
      val typeName: String
  ) extends TypeMeta

  case object NullType extends PrimitiveTypeMeta("scala", "NULL")

  case object IntegerType extends PrimitiveTypeMeta("scala", "Int")

  case object StringType extends PrimitiveTypeMeta("scala", "String")

  case object LongType extends PrimitiveTypeMeta("scala", "Long")

  case object DoubleType extends PrimitiveTypeMeta("scala", "Double")

  case object FloatType extends PrimitiveTypeMeta("scala", "Float")

  case object ShortType extends PrimitiveTypeMeta("scala", "Short")

  case object ByteType extends PrimitiveTypeMeta("scala", "Byte")

  case object BooleanType extends PrimitiveTypeMeta("scala", "Boolean")

  case object BinaryType extends PrimitiveTypeMeta("scala", "Binary")

  case object TimestampType extends PrimitiveTypeMeta("java.sql", "Timestamp")

  case object DateType extends PrimitiveTypeMeta("java.util", "Date")

  case object BigDecimalType extends PrimitiveTypeMeta("scala", "BigDecimal")

  case object BigIntegerType extends PrimitiveTypeMeta("scala.math", "BigInt")

  case class SeqTypeMeta(element: TypeMeta) extends TypeMeta {
    override val packageName: String = "scala"
    override val typeName: String    = "Seq"
  }

  case class MapTypeMeta(key: TypeMeta, value: TypeMeta) extends TypeMeta {
    override val packageName: String = "scala"
    override val typeName: String    = "Map"
  }

  /*
  case class RecordTypeMeta(records: Iterable[TypeMeta]) extends TypeMeta {
    override val packageName: String = "hive"
    override val typeName: String = "Record"
  }

   */

  case class ClassFieldMeta(
      /**
        * Original Scala class field name
        */
      originalFieldName: String,
      /**
        * Field name after applying annotations
        */
      fieldName: String,
      fieldType: TypeMeta,
      annotations: Iterable[AnnotationMeta]
  ) extends HasAnnotations

  /*
   * ClassTypeMeta puts together the primitive and complex types defined above along with annotations into an object
   * that structures all the information that is required.
   *
   * */

  case class ClassTypeMeta(
      packageName: String,
      /**
        * Name after applying annotations
        *
        * */

      typeName: String,
      /**
        * Original Scala class name
        * */

      originalTypeName: String,
      annotations: Iterable[AnnotationMeta],
      fields: Iterable[ClassFieldMeta]
  ) extends TypeMeta
      with HasAnnotations

}
