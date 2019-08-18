package io.pascals.avro.schema.utils

import io.pascals.avro.schema.metadata._
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

class AvroDiffUtilsTest extends FunSuite with Matchers {

  val parserOne = new Parser
  val parserTwo = new Parser
  val parserThree = new Parser

  val testSchemaOne: Schema = parserOne.parse(this.getClass.getResourceAsStream("/old/test_acquisition_traffic_scoring.avsc"))
  val testSchemaTwo: Schema = parserTwo.parse(this.getClass.getResourceAsStream("/new/test_acquisition_traffic_scoring.avsc"))


  val diffString: String =
    """{"type":"record","name":"test_acquisition_traffic_scoring","namespace":"io.pascals.avro.schema.model.acquisition","fields":[{"name":"event_data_new_score","type":["null","int"],"default":null},{"name":"event_data_new_array","type":["null",{"type":"array","items":["null","int"]}],"default":null},{"name":"event_data_new_decision","type":["null","string"],"default":null},{"name":"event_data_new_strategies","type":["null",{"type":"array","items":["null",{"type":"record","name":"null","fields":[{"name":"strategy","type":["null","string"]},{"name":"score","type":["null","int"]},{"name":"decision","type":["null","string"]}]}]}],"default":null},{"name":"event_data_new_map_column","type":["null",{"type":"map","values":["null","string"]}],"default":null}]}""".stripMargin

  val diffSchema: Schema = parserThree.parse(diffString)
  val diff: Option[Schema] = AvroDiffUtils.diffAvroSchemas(testSchemaOne, testSchemaTwo)
  val diffClassTypeMeta = ClassTypeMeta("test_acquisition_traffic_scoring", "test_acquisition_traffic_scoring", "test_acquisition_traffic_scoring", List(), List(ClassFieldMeta("event_data_new_decision","event_data_new_decision",StringType,List()), ClassFieldMeta("event_data_new_score","event_data_new_score",IntegerType,List()), ClassFieldMeta("event_data_new_strategies","event_data_new_strategies",SeqTypeMeta(ClassTypeMeta("union","union","union",List(),ArrayBuffer(ClassFieldMeta("strategy","strategy",StringType,List()), ClassFieldMeta("score","score",IntegerType,List()), ClassFieldMeta("decision","decision",StringType,List())))),List()), ClassFieldMeta("event_data_new_map_column","event_data_new_map_column",MapTypeMeta(StringType,StringType),List()), ClassFieldMeta("event_data_new_array","event_data_new_array",SeqTypeMeta(IntegerType),List())))

  test("Avro diff schema Test") {
    val inequalityTest: Option[Schema] = diff match {
      case Some(s) => AvroDiffUtils.diffAvroSchemas(s, diffSchema)
      case None =>  None
    }
    inequalityTest shouldBe None
  }

  test("ClassTypeMeta generation Test") {
    val classTypeMeta:  TypeMeta = diff match {
      case Some(sc) =>  AvroDiffUtils.typeMetaGenerator(sc, sc.getName)._2 //Some(AvroDiffServiceImpl.genDiffClassTypeMeta(s))
      case None =>  NullType
    }

    classTypeMeta match {
      case cls: ClassTypeMeta =>
        val clsFields: Iterable[(String, TypeMeta)] = cls.fields.map {
          f => (f.fieldName, f.fieldType)
        }
        val diffClsFields: Iterable[(String, TypeMeta)] = diffClassTypeMeta.fields.map {
          f => (f.fieldName, f.fieldType)
        }
        clsFields.map { f => f._1 }.toSet shouldEqual diffClsFields.map { f => f._1 }.toSet
        clsFields.map { f => f._2 }.toSet shouldEqual diffClsFields.map { f => f._2 }.toSet
      case _ => assertThrows("ClassTypeMeta generation error")
    }


  }


}
