package io.pascals.avro.schema.metadata

import io.pascals.avro.schema.annotations.hive._
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ListBuffer

class MetadataExtractorTest extends FunSuite with Matchers {

  @hiveTable(name = "PersonUnitTest")
  case class Person(
      @column(name = "name") personName: String,
      @column(name = "age") personAge: Int
  )

  test("Extract Metadata Test") {
    val personMeta = MetadataExtractor.extractClassMeta[Person]()
    personMeta.typeName should equal("Person")
    personMeta.fields.toList.head.fieldName should equal("personName")
    personMeta.fields.toList(1).fieldName should equal("personAge")
    personMeta.annotations.toList.head.name should equal(
      "io.pascals.avro.schema.annotations.hive.hiveTable"
    )
    personMeta.annotations.toList.head.getValue should equal("PersonUnitTest")

    val fieldNamesBuffer: ListBuffer[String] = ListBuffer[String]()
    val fieldAnnotationsBuffer: ListBuffer[AnnotationMeta] =
      ListBuffer[AnnotationMeta]()
    personMeta.fields.foreach { f =>
      {
        fieldNamesBuffer += f.fieldName
        fieldAnnotationsBuffer ++= f.annotations
      }
    }

    fieldNamesBuffer.head should equal("personName")
    fieldNamesBuffer(1) should equal("personAge")

    val annotationsBuffer: ListBuffer[String] = ListBuffer[String]()
    val annotationsAttributeBuffer: ListBuffer[AnnotationAttribute] =
      ListBuffer[AnnotationAttribute]()
    fieldAnnotationsBuffer.foreach { annotationMeta =>
      {
        annotationsBuffer += annotationMeta.name
        annotationsAttributeBuffer ++= annotationMeta.attributes
      }
    }

    annotationsBuffer.head should equal(
      "io.pascals.avro.schema.annotations.hive.column"
    )
    annotationsBuffer(1) should equal(
      "io.pascals.avro.schema.annotations.hive.column"
    )

    annotationsAttributeBuffer.head.value should equal("name")
    annotationsAttributeBuffer(1).value should equal("age")
  }
}
