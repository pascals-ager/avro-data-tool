package io.pascals.avro.schema.service

import cats.effect.{ContextShift, IO}
import doobie.free.connection.{close, unit}
import doobie.implicits._
import doobie.util.transactor.{Strategy, Transactor}
import io.pascals.avro.schema.annotations.hive._
import io.pascals.avro.schema.metadata._
import io.pascals.avro.schema.service.HiveExecutorServiceImpl._
import io.pascals.avro.schema.service.HiveGeneratorServiceImpl.generateCreateDDL
import io.pascals.avro.schema.tags.IntegrationTest
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext
import scala.util.{Failure, Success, Try}

class HiveExecutorServiceImplTest extends FunSuite with Matchers {

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  val hiveStrategy: Strategy = Strategy.void.copy(
    after = unit, oops = unit, always = close)

  val xa: Transactor[IO] = Transactor.strategy.set(
    Transactor.fromDriverManager[IO]("org.apache.hive.jdbc.HiveDriver", "jdbc:hive2://hive-server-two:10000",
      "hive", "hive"),
    hiveStrategy
  )

  case class Person(
                     personName: String,
                     personAge: Int,
                     personRelationships: Map[String, String],
                     @hiveBucket(buckets = 6)
                     @column(name = "tracking_id")
                     personReference: String,
                     personRole: String,
                     @hivePartitionColumn
                     year: Int,
                     @hivePartitionColumn
                     month: Int,
                     @hivePartitionColumn
                     day: Int)

  test("Create Simple Person Table Test", IntegrationTest)  {
    for {
      ddlConnection <- executeCreateHiveTable[Person]
    } yield ddlConnection.transact(xa).unsafeRunSync() shouldEqual 0
  }

  case class source_attributes(id: String, origin: String, internal_data: Option[Map[String, Option[String]]], external_data: Option[Map[String, Option[String]]])
  case class test_acquisition_traffic_scoring(`type`: String, id: String, happened: String, processed: String, tracking_id: String, referenced_event_id: Option[String], source_attributes: source_attributes, event_data_score: Int, event_data_decision: String, event_data_reason: Option[String])

  test("Create test_acquisition_traffic_scoring Table Test", IntegrationTest) {
    for {
      ddlConnection <- executeCreateHiveTable[test_acquisition_traffic_scoring]
    } yield ddlConnection.transact(xa).unsafeRunSync() shouldEqual 0
  }

  val diffClassTypeMeta = ClassTypeMeta("test_acquisition_traffic_scoring","test_acquisition_traffic_scoring","test_acquisition_traffic_scoring",List(),ArrayBuffer(ClassFieldMeta("event_data_new_strategies","event_data_new_strategies",SeqTypeMeta(StringType),List()), ClassFieldMeta("event_data_new_map_column","event_data_new_map_column",MapTypeMeta(StringType,StringType),List()), ClassFieldMeta("event_data_new_array","event_data_new_array",SeqTypeMeta(IntegerType),List()), ClassFieldMeta("event_data_new_decision","event_data_new_decision",StringType,List()), ClassFieldMeta("event_data_new_score","event_data_new_score",IntegerType,List())))

  test("Alter test_acquisition_traffic_scoring Test", IntegrationTest) {
    val execute: Option[Try[Int]] = for {
      ddlConnection <- executeAlterHiveTable(diffClassTypeMeta)
    } yield Try(ddlConnection.transact(xa).unsafeRunSync())

    execute match  {
      case Some(Success(status)) => status shouldEqual 0
      case Some(Failure(throwable: Throwable)) =>
        fail(s"Hive Server returned error while executing ALTER Table statement - ${throwable.toString}")
      case None => fail("Unable to execute ALTER Table statement")
    }
  }

  case class ReferencedPerson(
                               personName: Option[String],
                               personAge: Option[Int],
                               personRelationships: Option[List[Option[Relationship]]],
                               personReference: Option[String],
                               personRole: Option[String])

  case class Relationship(fromPerson: String, toPerson: String, relationshipType: Option[String])

  test("Create ReferencedPerson with simple executor service Test", IntegrationTest){
    for {
      createDDL <- generateCreateDDL[ReferencedPerson]()
      connection <- hiveExecutorService(createDDL)
    } yield connection.transact(xa).unsafeRunSync() shouldEqual 0
  }

  /*
  case class TestReferencedPerson(
                               personName: Option[String],
                               personAge: Option[Int],
                               personRelationships: Option[List[Option[Relationship]]],
                               personReference: Option[String],
                               personRole: Option[String])

  test("Create ReferencedPerson with test executor service Test", IntegrationTest){
    for {
      createDDL <- generateCreateDDL[TestReferencedPerson]()
      connection <- Option(hiveTestExecutorService(createDDL))
    } yield connection.transact(xa).unsafeRunSync shouldEqual Some(0)
  }
  */
}
