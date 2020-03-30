package io.github.pascals.avro.schema.service

import doobie.free.connection.ConnectionIO
import io.github.pascals.avro.schema.metadata._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait QueryExecutor {

  /**
    * Generates Hive Alter Table statement for provided ClassTypeMeta and returns a side effect free ConnectionIO.
    * @param cls class metadata
    */
  def executeAlterHiveTable(cls: ClassTypeMeta): Option[ConnectionIO[Int]]

  /**
    * Generates data model for provided type and creates new Hive table.
    * @tparam T type for which to generate and returns a side effect free ConnectionIO.
    * @param default Generate default table format, buckets and properties
    */
  def executeCreateHiveTable[T: ClassTag: TypeTag](
      default: Boolean = false
  ): Option[ConnectionIO[Int]]

  /**
    *
    */
  def hiveExecutorService: String => Option[ConnectionIO[Int]]
}
