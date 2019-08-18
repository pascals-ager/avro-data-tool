package io.pascals.avro.schema.service

import doobie.free.connection.ConnectionIO
import io.pascals.avro.schema.metadata._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag


trait QueryExecutorService {

  /**
    * Generates Hive Alter Table statement for provided ClassTypeMeta and returns a side effect free ConnectionIO.
    *
    */
  val executeAlterHiveTable:  ClassTypeMeta =>  Option[ConnectionIO[Int]]

  /**
    * Generates data model for provided type and creates new Hive table.
    * @tparam T type for which to generate and returns a side effect free ConnectionIO.
    */
  def executeCreateHiveTable[T: ClassTag : TypeTag]: Option[ConnectionIO[Int]]

  /**
    *
    */
  val hiveExecutorService: String => Option[ConnectionIO[Int]]
}
