package io.pascals.avro.schema.dialects

import io.pascals.avro.schema.metadata._

import scala.util.Try

object HiveDialect extends Dialect {

  /*
   * HiveDialect transforms ClassTypeMeta into implementation specific Hive statements.
   * We first model data types that Hive Dialect understands and create a mapping from the various types in ClassTypeMeta
   * to the types defined in Hive Dialect.
   * */

  def intType: String = "INT"

  def stringType: String = "STRING"

  def longType: String = "BIGINT"

  def doubleType: String = "DOUBLE"

  def floatType: String = "FLOAT"

  def shortType: String = "SMALLINT"

  def booleanType: String = "BOOLEAN"

  def byteType: String = "TINYINT"

  def dateType: String = "DATE"

  def timestampType: String = "TIMESTAMP"

  def binaryType: String = "BINARY"

  def bigDecimalType: String = "DECIMAL(38,18)"

  def bigIntegerType: String = "BIGINT"

  /*
   * Model special characters and reserved keywords in Hive. These must be sanitized from a field name.
   * */

  val specialCharacters = Seq(
    " ",
    "\\",
    "!",
    "@",
    "#",
    "%",
    "^",
    "&",
    "*",
    "(",
    ")",
    "-",
    "+",
    "=",
    "[",
    "]",
    "{",
    "}",
    ";",
    ":",
    "'",
    "\"",
    ",",
    ".",
    "<",
    ">",
    "/",
    "?",
    "|",
    "~"
  )

  val reservedKeywords = Seq(
    "ALL",
    "ALTER",
    "AND",
    "AS",
    "BETWEEN",
    "CASE",
    "COLUMN",
    "CREATE",
    "DATABASE",
    "DATE",
    "DELETE",
    "DISTINCT",
    "DROP",
    "ELSE",
    "END",
    "EXISTS",
    "FALSE",
    "FETCH",
    "FULL",
    "FUNCTION",
    "GRANT",
    "GROUP",
    "HAVING",
    "INNER",
    "INSERT",
    "INTO",
    "JOIN",
    "LEFT",
    "NOT",
    "NULL",
    "OR",
    "ORDER",
    "OUTER",
    "SELECT",
    "TABLE",
    "TRUE",
    "UNION",
    "UPDATE",
    "USER",
    "USING",
    "VALUES",
    "WHEN",
    "WHERE"
  )

  /*
   * ClassTypeMeta object also implement HasAnnotations. If annotations exist, they must be applied to get the desired
   * type and field names
   * */

  override def applyAnnotation(c: ClassTypeMeta): ClassTypeMeta = {
    c.copy(
      typeName = getAnnotatedClassName(c),
      fields = c.fields.map(f => applyAnnotation(f, c))
    )
  }

  private def applyAnnotation(
      cf: ClassFieldMeta,
      c: ClassTypeMeta
  ): ClassFieldMeta = {
    cf.copy(
      fieldName = getAnnotatedFieldName(cf, c),
      fieldType = applyAnnotation(cf.fieldType)
    )
  }

  private def applyAnnotation(typeMetaData: TypeMeta): TypeMeta =
    typeMetaData match {
      case p: PrimitiveTypeMeta => p
      case c: SeqTypeMeta       => c
      case m: MapTypeMeta       => m
      case c: ClassTypeMeta     => applyAnnotation(c)
    }

  override def getSpecificAnnotation(
      annotationName: String,
      hasAnnotations: HasAnnotations
  ): Option[AnnotationMeta] = {
    val annotation: Option[AnnotationMeta] =
      hasAnnotations.annotations.find(a => a.name == annotationName)
    annotation
  }

  /*
   * With the mappings defined and annotations applied, we implement generators which know what to do with a ClassTypeMeta.
   * */

  override def generateArrayTypeExpression(
      elementTypeExpression: String
  ): String =
    s"ARRAY<$elementTypeExpression>"

  def generateClassTypeExpression(
      classTypeMetaData: ClassTypeMeta,
      fieldNamesWithExpressions: Iterable[(String, String)]
  ): String =
    s"STRUCT<${classTypeMetaData.fields
      .map(f => s"${f.fieldName} : ${generateTypeExpression(f.fieldType)}")
      .mkString(", ")}>"

  override def generateMapTypeExpression(
      keyExpression: String,
      valueExpression: String
  ): String =
    s"MAP<$keyExpression, $valueExpression>"

  override def generateTypeExpression(typeMetaData: TypeMeta): String =
    generateTypeExpression(typeMetaData, 0)

  private def generateTypeExpression(
      typeMetaData: TypeMeta,
      level: Int
  ): String = typeMetaData match {
    case p: PrimitiveTypeMeta => generatePrimitiveTypeExpression(p)
    case c: SeqTypeMeta =>
      generateArrayTypeExpression(generateTypeExpression(c.element))
    case m: MapTypeMeta =>
      generateMapTypeExpression(
        generateTypeExpression(m.key),
        generateTypeExpression(m.value, level)
      )
    case c: ClassTypeMeta =>
      generateClassTypeExpression(
        c,
        c.fields.map(
          f => (f.fieldName, generateClassFieldExpression(f, level + 1))
        )
      )
  }

  /*
   * Provide implementations for specific annotations
   * TODO: Move this to some kind of typeclass pattern
   * */

  private val HiveTable: String =
    "io.pascals.avro.schema.annotations.hive.hiveTable"
  private val HiveExternalTable: String =
    "io.pascals.avro.schema.annotations.hive.hiveExternalTable"
  private val Column     = "io.pascals.avro.schema.annotations.hive.column"
  private val Underscore = "io.pascals.avro.schema.annotations.hive.underscore"
  private val HivePartitionColumn: String =
    "io.pascals.avro.schema.annotations.hive.hivePartitionColumn"
  private val HiveBucketColumn: String =
    "io.pascals.avro.schema.annotations.hive.hiveBucket"
  private val HiveStoredAs: String =
    "io.pascals.avro.schema.annotations.hive.hiveStoredAs"
  private val HiveTblProps: String =
    "io.pascals.avro.schema.annotations.hive.hiveTableProperty"

  override def alterDataModel(
      classTypeMeta: ClassTypeMeta,
      fieldsExpressions: Iterable[String]
  ): String =
    alterTableExpression(classTypeMeta) +
      generateColumnsExpression(classTypeMeta, fieldsExpressions)

  override def generateDataModel(
      classTypeMeta: ClassTypeMeta,
      fieldsExpressions: Iterable[String]
  ): String =
    createTableExpression(classTypeMeta) +
      generateColumnsExpression(classTypeMeta, fieldsExpressions) +
      generatePartitionExpressions(classTypeMeta) +
      generateBucketExpressions(classTypeMeta) +
      generateStoredAsExpression(classTypeMeta) +
      generateTblPropsExpression(classTypeMeta)

  override def getAnnotatedClassName(c: ClassTypeMeta): String = {
    val dialectSpecificTableAnnotation = getSpecificAnnotation(HiveTable, c)
    dialectSpecificTableAnnotation
      .map(a => a.attributes.filter(_.name == "name").head.value)
      .getOrElse(convertToUnderscoreIfRequired(c.typeName, c))
  }

  override def getAnnotatedFieldName(
      cf: ClassFieldMeta,
      c: ClassTypeMeta
  ): String = {
    val columnAnnotations = getSpecificAnnotation(Column, cf)
    columnAnnotations
      .map(a => a.attributes.filter(_.name == "name").head.value)
      .getOrElse(convertToUnderscoreIfRequired(cf.fieldName, c))
  }

  private def hiveExternalTableLocation(
      classTypeMetaData: ClassTypeMeta
  ): Option[String] =
    classTypeMetaData.getAnnotationValue(HiveExternalTable)

  override def generateClassFieldExpression(f: ClassFieldMeta): String =
    generateClassFieldExpression(f, 0)

  private def generateClassFieldExpression(
      f: ClassFieldMeta,
      level: Int
  ): String = {
    val typeExpression = generateTypeExpression(f.fieldType)
    generateClassFieldExpression(f, typeExpression, level)
  }

  private def generateClassFieldExpression(
      f: ClassFieldMeta,
      typeExpression: String,
      level: Int
  ): String =
    generateFieldName(f.fieldName) + " " + typeExpression

  override def generateFieldName(columnName: String): String =
    if (reservedKeywords.contains(columnName.toUpperCase))
      escapeColumnName(columnName)
    else if (specialCharacters.exists(columnName.contains(_)))
      escapeColumnName(columnName)
    else
      columnName

  private def escapeColumnName(columnName: String) =
    s"`$columnName`"

  override def alterTableExpression(classTypeMetaData: ClassTypeMeta): String =
    s"ALTER TABLE ${classTypeMetaData.typeName} ADD COLUMNS"

  override def createTableExpression(classTypeMetaData: ClassTypeMeta): String =
    s"CREATE ${if (hiveExternalTableLocation(classTypeMetaData).isDefined) "EXTERNAL "
    else ""}TABLE IF NOT EXISTS ${classTypeMetaData.typeName}"

  private def convertToUnderscoreIfRequired(
      name: String,
      c: ClassTypeMeta
  ): String = {
    val underscoreAnnotation: Option[AnnotationMeta] =
      getSpecificAnnotation(Underscore, c)
    underscoreAnnotation
      .map(a => name.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase)
      .getOrElse(name)
  }

  private def isPartitionField(field: ClassFieldMeta): Boolean =
    field.annotations
      .exists(_.name == HivePartitionColumn)

  def generateColumn(f: ClassFieldMeta): Boolean =
    !isPartitionField(f)

  private def getPartitionFields(c: ClassTypeMeta): Seq[ClassFieldMeta] = {
    val partitionFields =
      c.fields.filter(_.annotations.exists(_.name == HivePartitionColumn))
    var fieldOrder = 0
    partitionFields
      .map(f => {
        val partitionColumn =
          f.annotations.find(_.name == HivePartitionColumn).get
        val order = Try(
          partitionColumn.attributes.find(_.name == "order").get.value.toInt
        ).getOrElse(0)
        fieldOrder += 1
        (f, order, fieldOrder)
      })
      .toSeq
      .sortWith {
        case (e1, e2) => e1._2 < e2._2 || (e1._2 == e2._2 && e1._3 < e2._3)
      }
      .map(_._1)
  }

  private def generatePartitionExpressions(c: ClassTypeMeta): String = {
    val partitionFields = getPartitionFields(c)
    if (partitionFields.isEmpty)
      ""
    else {
      var fieldOrder = 0
      "\nPARTITIONED BY(" +
        partitionFields
          .map(f => s"${f.fieldName} ${generateTypeExpression(f.fieldType)}")
          .mkString(", ") +
        ")"
    }
  }

  private def getBucketedFields(
      c: ClassTypeMeta
  ): Seq[(ClassFieldMeta, Int)] = {
    val bucketedFeilds: Iterable[ClassFieldMeta] =
      c.fields.filter(_.annotations.exists(_.name == HiveBucketColumn))
    bucketedFeilds
      .map(f => {
        val bucketColumn: AnnotationMeta =
          f.annotations.find(_.name == HiveBucketColumn).get
        val buckets = Try(
          bucketColumn.attributes.find(_.name == "buckets").get.value.toInt
        ).getOrElse(1)
        (f, buckets)
      })
      .toSeq
  }

  private def generateBucketExpressions(c: ClassTypeMeta): String = {
    val bucketedFields = getBucketedFields(c)
    if (bucketedFields.isEmpty)
      ""
    else {
      var numOfBuckets = 0
      "\nCLUSTERED BY (" +
        bucketedFields
          .map(f => {
            numOfBuckets = numOfBuckets + f._2
            s"${f._1.fieldName}"
          })
          .mkString(", ") +
        ")" +
        s"\nINTO $numOfBuckets BUCKETS"
    }
  }

  private def getStoredAsProperty(c: ClassTypeMeta): Option[String] = {
    val storedAsProp = getSpecificAnnotation(HiveStoredAs, c)
    storedAsProp.map(a => a.attributes.filter(_.name == "format").head.value)

  }

  private def generateStoredAsExpression(c: ClassTypeMeta): String = {
    getStoredAsProperty(c) match {
      case Some(format) => s"\nSTORED AS $format"
      case None         => ""
    }
  }

  private def getTblProperties(c: ClassTypeMeta): Iterable[(String, String)] = {
    c.annotations.filter(_.name == HiveTblProps).map { props =>
      {
        (
          props.attributes.find(_.name == "key").get.value,
          props.attributes.find(_.name == "value").get.value
        )
      }
    }
  }

  private def generateTblPropsExpression(c: ClassTypeMeta): String = {
    val tblProps = getTblProperties(c)
    if (tblProps.isEmpty) ""
    else
      "\nTBLPROPERTIES(\n  " +
        tblProps.map { props =>
          s"'${props._1}' = '${props._2}'"
        }.mkString(",\n  ") +
        "\n)"
  }

}
