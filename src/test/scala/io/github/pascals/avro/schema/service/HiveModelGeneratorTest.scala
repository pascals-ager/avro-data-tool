package io.github.pascals.avro.schema.service

import io.github.pascals.avro.schema.annotations.hive._
import io.github.pascals.avro.schema.metadata._
import io.github.pascals.avro.schema.service.HiveModelGenerator.{
  generateAlterDDL,
  generateCreateDDL
}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable.ArrayBuffer

class HiveModelGeneratorTest extends FunSuite with Matchers {

  @hiveTable(name = "Person")
  case class Person(
      @column(name = "name")
      personName: String,
      @column(name = "age")
      personAge: Int,
      @column(name = "relationship")
      personRelationships: Map[String, String],
      @hiveBucket(buckets = 6)
      @column(name = "tracking_id")
      personReference: String,
      @hiveBucket(buckets = 4)
      @column(name = "position")
      personRole: String,
      @hivePartitionColumn
      year: Int,
      @hivePartitionColumn
      month: Int,
      @hivePartitionColumn
      day: Int
  )

  val personDDL: String =
    """CREATE TABLE IF NOT EXISTS Person(
      |   name STRING,
      |   age INT,
      |   relationship MAP<STRING, STRING>,
      |   tracking_id STRING,
      |   position STRING
      |)
      |PARTITIONED BY(year INT, month INT, day INT)
      |CLUSTERED BY (tracking_id, position)
      |INTO 10 BUCKETS""".stripMargin

  test("Generate Annotated Person Table Test") {

    val person: String = generateCreateDDL[Person]().get
    person should equal(personDDL)

  }

  case class SimplePerson(
      personName: String,
      personAge: Int,
      personRelationships: Map[String, String],
      personReference: String,
      personRole: String,
      year: Int,
      month: Int,
      day: Int
  )

  val simplePersonDDL: String =
    """CREATE TABLE IF NOT EXISTS SimplePerson(
      |   personName STRING,
      |   personAge INT,
      |   personRelationships MAP<STRING, STRING>,
      |   personReference STRING,
      |   personRole STRING,
      |   year INT,
      |   month INT,
      |   day INT
      |)""".stripMargin

  test("Generate Simple Person Table Test") {

    val simplePerson: String = generateCreateDDL[SimplePerson]().get
    simplePerson should equal(simplePersonDDL)

  }

  case class ReferencedPerson(
      personName: Option[String],
      personAge: Option[Int],
      personRelationships: Option[List[Option[Relationship]]],
      personReference: Option[String],
      personRole: Option[String]
  )

  case class Relationship(
      fromPerson: String,
      toPerson: String,
      relationshipType: Option[String]
  )

  val referencedPersonDDL: String =
    """CREATE TABLE IF NOT EXISTS ReferencedPerson(
      |   personName STRING,
      |   personAge INT,
      |   personRelationships ARRAY<STRUCT<fromPerson : STRING, toPerson : STRING, relationshipType : STRING>>,
      |   personReference STRING,
      |   personRole STRING
      |)""".stripMargin

  test("Generate Referenced Person Table Test") {

    val referencedPerson: String = generateCreateDDL[ReferencedPerson]().get
    referencedPerson should equal(referencedPersonDDL)

  }

  case class test_click(
      year: Int,
      month: Int,
      day: Int,
      `type`: String,
      id: String,
      happened: String,
      processed: String,
      tracking_id: String,
      source_attributes: source_attributes,
      event_data_remote_address: Option[String],
      event_data_query_parameters: Option[Map[String, Seq[String]]],
      event_data_headers: Option[Map[String, Option[String]]],
      event_data_device: Option[event_data_device],
      event_data_geolocation: Option[event_data_geolocation],
      event_data_isp: Option[event_data_isp]
  )
  case class event_data_device(
      device_manufacturer: Option[String],
      device_name: Option[String],
      form_factor: Option[String],
      os: Option[String],
      os_version: Option[String],
      is_tablet: Option[Boolean],
      is_wireless_device: Option[Boolean],
      mobile_browser: Option[String],
      mobile_browser_version: Option[String]
  )
  case class event_data_geolocation(
      latitude: Option[String],
      longitude: Option[String],
      precision_radius: Option[Int],
      continent: Option[String],
      country_code: Option[String],
      region: Option[String],
      city: Option[String],
      city_localized: Option[String],
      zipcode: Option[String],
      timezone: Option[String]
  )
  case class event_data_isp(
      network: Option[String],
      name: Option[String],
      organization: Option[String],
      autonomous_system_number: Option[String]
  )
  case class source_attributes(
      id: String,
      origin: String,
      internal_data: Option[Map[String, Option[Int]]],
      external_data: Option[Map[String, Option[String]]]
  )

  val testClickDDL: String =
    """CREATE TABLE IF NOT EXISTS test_click(
                            |   year INT,
                            |   month INT,
                            |   day INT,
                            |   type STRING,
                            |   id STRING,
                            |   happened STRING,
                            |   processed STRING,
                            |   tracking_id STRING,
                            |   source_attributes STRUCT<id : STRING, origin : STRING, internal_data : MAP<STRING, INT>, external_data : MAP<STRING, STRING>>,
                            |   event_data_remote_address STRING,
                            |   event_data_query_parameters MAP<STRING, ARRAY<STRING>>,
                            |   event_data_headers MAP<STRING, STRING>,
                            |   event_data_device STRUCT<device_manufacturer : STRING, device_name : STRING, form_factor : STRING, os : STRING, os_version : STRING, is_tablet : BOOLEAN, is_wireless_device : BOOLEAN, mobile_browser : STRING, mobile_browser_version : STRING>,
                            |   event_data_geolocation STRUCT<latitude : STRING, longitude : STRING, precision_radius : INT, continent : STRING, country_code : STRING, region : STRING, city : STRING, city_localized : STRING, zipcode : STRING, timezone : STRING>,
                            |   event_data_isp STRUCT<network : STRING, name : STRING, organization : STRING, autonomous_system_number : STRING>
                            |)""".stripMargin

  test("Generate Clicks Table Test") {
    val testClick: String = generateCreateDDL[test_click]().get
    testClick should equal(testClickDDL)
  }

  @hiveStoredAs(format = "ORC")
  @hiveTable(name = "TestOrc")
  case class Test(name: String, detail: Int)

  val storedAsDDL: String = """CREATE TABLE IF NOT EXISTS TestOrc(
                              |   name STRING,
                              |   detail INT
                              |)
                              |STORED AS ORC""".stripMargin

  test("Generate Stored as property Test") {

    val storedAsTable: String = generateCreateDDL[Test]().get
    storedAsTable should equal(storedAsDDL)

  }

  @hiveTableProperty("orc.compress", "ZLIB")
  @hiveTableProperty("orc.compression.strategy", "SPEED")
  @hiveTableProperty("orc.create.index", "true")
  @hiveTableProperty("orc.encoding.strategy", "SPEED")
  @hiveTableProperty("transactional", "true")
  @hiveTableProperty("avro.schema.url", "hdfs:///metadata/table.avro")
  @hiveTable(name = "TestProperty")
  case class PropertyTest(name: String, detail: Int)

  val propertyDDL: String = """CREATE TABLE IF NOT EXISTS TestProperty(
                              |   name STRING,
                              |   detail INT
                              |)
                              |TBLPROPERTIES(
                              |  'orc.compress' = 'ZLIB',
                              |  'orc.compression.strategy' = 'SPEED',
                              |  'orc.create.index' = 'true',
                              |  'orc.encoding.strategy' = 'SPEED',
                              |  'transactional' = 'true',
                              |  'avro.schema.url' = 'hdfs:///metadata/table.avro'
                              |)""".stripMargin

  test("Generate TBLPROPERTIES Test") {

    val propertyTable: String = generateCreateDDL[PropertyTest]().get
    propertyTable should equal(propertyDDL)

  }

  @hiveTableProperty("orc.compress", "ZLIB")
  @hiveTableProperty("orc.compression.strategy", "SPEED")
  @hiveTableProperty("orc.create.index", "true")
  @hiveTableProperty("orc.encoding.strategy", "SPEED")
  @hiveTableProperty("transactional", "true")
  @hiveStoredAs(format = "ORC")
  @hiveTable(name = "test_events_acquisition_click")
  case class acquisition_click_test(
      @hivePartitionColumn year: Int,
      @hivePartitionColumn month: Int,
      @hivePartitionColumn day: Int,
      `type`: String,
      id: String,
      happened: String,
      processed: String,
      @hiveBucket(buckets = 6)
      tracking_id: String,
      source_attributes: source_attributes,
      event_data_remote_address: Option[String],
      event_data_query_parameters: Option[Map[String, Seq[String]]],
      event_data_headers: Option[Map[String, Option[String]]],
      event_data_device: Option[event_data_device],
      event_data_geolocation: Option[event_data_geolocation],
      event_data_isp: Option[event_data_isp]
  )

  val acquisitionClicksTestDDL: String =
    """CREATE TABLE IF NOT EXISTS test_events_acquisition_click(
                                |   type STRING,
                                |   id STRING,
                                |   happened STRING,
                                |   processed STRING,
                                |   tracking_id STRING,
                                |   source_attributes STRUCT<id : STRING, origin : STRING, internal_data : MAP<STRING, INT>, external_data : MAP<STRING, STRING>>,
                                |   event_data_remote_address STRING,
                                |   event_data_query_parameters MAP<STRING, ARRAY<STRING>>,
                                |   event_data_headers MAP<STRING, STRING>,
                                |   event_data_device STRUCT<device_manufacturer : STRING, device_name : STRING, form_factor : STRING, os : STRING, os_version : STRING, is_tablet : BOOLEAN, is_wireless_device : BOOLEAN, mobile_browser : STRING, mobile_browser_version : STRING>,
                                |   event_data_geolocation STRUCT<latitude : STRING, longitude : STRING, precision_radius : INT, continent : STRING, country_code : STRING, region : STRING, city : STRING, city_localized : STRING, zipcode : STRING, timezone : STRING>,
                                |   event_data_isp STRUCT<network : STRING, name : STRING, organization : STRING, autonomous_system_number : STRING>
                                |)
                                |PARTITIONED BY(year INT, month INT, day INT)
                                |CLUSTERED BY (tracking_id)
                                |INTO 6 BUCKETS
                                |STORED AS ORC
                                |TBLPROPERTIES(
                                |  'orc.compress' = 'ZLIB',
                                |  'orc.compression.strategy' = 'SPEED',
                                |  'orc.create.index' = 'true',
                                |  'orc.encoding.strategy' = 'SPEED',
                                |  'transactional' = 'true'
                                |)""".stripMargin

  test("Generate Clicks Table With Annotations Test") {
    val acquisitionClicksTest: Option[String] =
      generateCreateDDL[acquisition_click_test]()
    acquisitionClicksTest match {
      case Some(createStmt) => createStmt shouldEqual acquisitionClicksTestDDL
      case None             => fail("Create Table statement not generated. Failing test!")
    }
  }

  val diffClassTypeMeta = ClassTypeMeta(
    "test_events_acquisition_traffic_scoring",
    "test_events_acquisition_traffic_scoring",
    "test_events_acquisition_traffic_scoring",
    List(),
    List(
      ClassFieldMeta(
        "event_data_new_decision",
        "event_data_new_decision",
        StringType,
        List()
      ),
      ClassFieldMeta(
        "event_data_new_score",
        "event_data_new_score",
        IntegerType,
        List()
      ),
      ClassFieldMeta(
        "event_data_new_strategies",
        "event_data_new_strategies",
        SeqTypeMeta(
          ClassTypeMeta(
            "union",
            "union",
            "union",
            List(),
            ArrayBuffer(
              ClassFieldMeta("strategy", "strategy", StringType, List()),
              ClassFieldMeta("score", "score", IntegerType, List()),
              ClassFieldMeta("decision", "decision", StringType, List())
            )
          )
        ),
        List()
      ),
      ClassFieldMeta(
        "event_data_new_map_column",
        "event_data_new_map_column",
        MapTypeMeta(StringType, StringType),
        List()
      ),
      ClassFieldMeta(
        "event_data_new_array",
        "event_data_new_array",
        SeqTypeMeta(IntegerType),
        List()
      )
    )
  )
  val alterTrafficScoringDDL: String =
    """ALTER TABLE test_events_acquisition_traffic_scoring ADD COLUMNS(
                                         |   event_data_new_decision STRING,
                                         |   event_data_new_score INT,
                                         |   event_data_new_strategies ARRAY<STRUCT<strategy : STRING, score : INT, decision : STRING>>,
                                         |   event_data_new_map_column MAP<STRING, STRING>,
                                         |   event_data_new_array ARRAY<INT>
                                         |)""".stripMargin

  test("Generate ALTER Table statement Test") {
    val alterDDL: Option[String] = generateAlterDDL(diffClassTypeMeta)
    alterDDL match {
      case Some(alterStmt) => alterStmt shouldEqual alterTrafficScoringDDL
      case None            => fail("Alter Table statement not generated. Failing test!")
    }
  }

}
