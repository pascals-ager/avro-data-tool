package io.github.pascals.avro.schema.dialects

import io.github.pascals.avro.schema.metadata.MetadataExtractor
import io.github.pascals.avro.schema.annotations.hive._
import io.github.pascals.avro.schema.metadata._
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ListBuffer

class HiveDialectTest extends FunSuite with Matchers {

  @hiveTable(name = "PersonUnitTest")
  case class Person(
      @column(name = "name") personName: String,
      @column(name = "age") personAge: Int
  )

  test("Apply Annotations Test") {
    val personMeta: ClassTypeMeta = MetadataExtractor.extractClassMeta[Person]()
    val personMetaWithAnnotations: ClassTypeMeta =
      HiveDialect.applyAnnotation(personMeta)

    personMetaWithAnnotations.typeName should equal("PersonUnitTest")
    personMetaWithAnnotations.originalTypeName should equal("Person")
    personMetaWithAnnotations.fields.toList.head.fieldName should equal("name")
    personMetaWithAnnotations.fields.toList(1).fieldName should equal("age")

    val fieldNamesBuffer: ListBuffer[String] = ListBuffer[String]()
    val fieldAnnotationsBuffer: ListBuffer[AnnotationMeta] =
      ListBuffer[AnnotationMeta]()
    personMetaWithAnnotations.fields.foreach { f =>
      {
        fieldNamesBuffer += f.fieldName
        fieldAnnotationsBuffer ++= f.annotations
      }
    }

    /*
     * Test the changes applied to the field names after applying annotations
     * */

    fieldNamesBuffer.head should equal("name")
    fieldNamesBuffer(1) should equal("age")

  }

}
