package io.pascals.avro.schema.dialects

import io.pascals.avro.schema.metadata._

trait Dialect {

  def intType: String

  def stringType: String

  def longType: String

  def doubleType: String

  def floatType: String

  def shortType: String

  def booleanType: String

  def byteType: String

  def dateType: String

  def timestampType: String

  def binaryType: String

  def bigDecimalType: String

  def bigIntegerType: String

  def mapPrimitiveDataType(primitiveType: PrimitiveTypeMeta): String =
    primitiveType match {
      case IntegerType    => intType
      case StringType     => stringType
      case LongType       => longType
      case DoubleType     => doubleType
      case FloatType      => floatType
      case ShortType      => shortType
      case BooleanType    => booleanType
      case ByteType       => byteType
      case DateType       => dateType
      case TimestampType  => timestampType
      case BinaryType     => binaryType
      case BigDecimalType => bigDecimalType
      case BigIntegerType => bigIntegerType
      case _              => throw new Exception("Not supported type: " + primitiveType)
    }

  /*
   * Model special characters and reserved keywords in Hive. These must be sanitized from a field name.
   * */

  val specialCharacters: Seq[String]

  val reservedKeywords: Seq[String]

  /*
   * ClassTypeMeta object also implement HasAnnotations. If annotations exist, they must be applied to get the desired
   * type and field names
   * */

  def applyAnnotation(c: ClassTypeMeta): ClassTypeMeta

  def getSpecificAnnotation(
      annotationName: String,
      hasAnnotations: HasAnnotations
  ): Option[AnnotationMeta]

  def getAnnotatedClassName(c: ClassTypeMeta): String

  def getAnnotatedFieldName(cf: ClassFieldMeta, c: ClassTypeMeta): String

  def generateClassTypeExpression(
      classTypeMetaData: ClassTypeMeta,
      fieldNamesWithExpressions: Seq[(String, String)]
  ): String

  def generateClassFieldExpression(f: ClassFieldMeta): String

  def generateColumnsExpression(
      classTypeMetaData: ClassTypeMeta,
      fieldsExpressions: Seq[String]
  ): String

  def generatePrimitiveTypeExpression(p: PrimitiveTypeMeta): String =
    mapPrimitiveDataType(p)

  def generateArrayTypeExpression(elementTypeExpression: String): String

  def generateMapTypeExpression(
      keyExpression: String,
      valueExpression: String
  ): String

  def generateTypeExpression(typeMetaData: TypeMeta): String

  def generateFieldName(columnName: String): String

  def alterDataModel(
      classTypeMeta: ClassTypeMeta,
      fieldsExpressions: Seq[String]
  ): String

  def generateDataModel(
      classTypeMeta: ClassTypeMeta,
      fieldsExpressions: Seq[String],
      default: Boolean = false
  ): String

  def alterTableExpression(classTypeMetaData: ClassTypeMeta): String

  def createTableExpression(classTypeMetaData: ClassTypeMeta): String
}
