package io.github.pascals.avro.schema.service

import io.github.pascals.avro.schema.metadata._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait ModelGenerator {

  /**
    * Generate data model for provided class
    *
    * @tparam T type for which to generate data model
    *
    * @param default Generate default table format, buckets and properties
    */
  def generateCreateDDL[T: ClassTag: TypeTag](
      default: Boolean = false
  ): Option[String]

  /**
    * Alter data model for provided class metadata
    *
    * @param cls extracted class metadata
    */
  def generateAlterDDL[T <: TypeMeta](cls: T): Option[String]
}
