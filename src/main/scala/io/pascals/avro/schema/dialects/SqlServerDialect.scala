package io.pascals.avro.schema.dialects

import io.pascals.avro.schema.metadata

object SqlServerDialect extends Dialect {
  override def intType: String = ???

  override def stringType: String = ???

  override def longType: String = ???

  override def doubleType: String = ???

  override def floatType: String = ???

  override def shortType: String = ???

  override def booleanType: String = ???

  override def byteType: String = ???

  override def dateType: String = ???

  override def timestampType: String = ???

  override def binaryType: String = ???

  override def bigDecimalType: String = ???

  override def bigIntegerType: String = ???

  override val specialCharacters: Seq[String] = ???

  override val reservedKeywords: Seq[String] = ???

  override def applyAnnotation(
      c: metadata.ClassTypeMeta
  ): metadata.ClassTypeMeta = ???

  override def getSpecificAnnotation(
      annotationName: String,
      hasAnnotations: metadata.HasAnnotations
  ): Option[metadata.AnnotationMeta] = ???

  override def getAnnotatedClassName(c: metadata.ClassTypeMeta): String = ???

  override def getAnnotatedFieldName(
      cf: metadata.ClassFieldMeta,
      c: metadata.ClassTypeMeta
  ): String = ???

  override def generateClassTypeExpression(
      classTypeMetaData: metadata.ClassTypeMeta,
      fieldNamesWithExpressions: Seq[(String, String)]
  ): String = ???

  override def generateColumnsExpression(
      classTypeMetaData: metadata.ClassTypeMeta,
      fieldsExpressions: Seq[String]
  ): String = ???

  override def generateClassFieldExpression(
      f: metadata.ClassFieldMeta
  ): String = ???

  override def generateTypeExpression(typeMetaData: metadata.TypeMeta): String =
    ???

  override def generateArrayTypeExpression(
      elementTypeExpression: String
  ): String = ???

  override def generateMapTypeExpression(
      keyExpression: String,
      valueExpression: String
  ): String = ???

  override def generateFieldName(columnName: String): String = ???

  override def alterDataModel(
      classTypeMeta: metadata.ClassTypeMeta,
      fieldsExpressions: Seq[String]
  ): String = ???

  override def alterTableExpression(
      classTypeMetaData: metadata.ClassTypeMeta
  ): String = ???

  override def createTableExpression(
      classTypeMetaData: metadata.ClassTypeMeta
  ): String = ???

  override def generateDataModel(
      classTypeMeta: metadata.ClassTypeMeta,
      fieldsExpressions: Seq[String],
      default: Boolean
  ): String = ???

}
