package io.pascals.avro.schema.service

import doobie.free.connection.ConnectionIO
import doobie.util.update.Update0
import io.pascals.avro.schema.metadata.ClassTypeMeta
import io.pascals.avro.schema.service.HiveGeneratorServiceImpl.{
  generateAlterDDL,
  generateCreateDDL
}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe

object HiveExecutorServiceImpl extends QueryExecutorService {

  /**
    * generates Hive Alter Table statement for provided ClassTypeMeta and returns a side effect free ConnectionIO.
    *
    *
    */
  override val executeAlterHiveTable
      : ClassTypeMeta => Option[ConnectionIO[Int]] =
    (classTypeMeta: ClassTypeMeta) =>
      for {
        alterDDL <- generateAlterDDL(classTypeMeta)
      } yield Update0(alterDDL, None).run

  /**
    * Generates data model for provided type and creates new Hive table.
    * @tparam T type for which to generate data model
    */
  override def executeCreateHiveTable[T: ClassTag: universe.TypeTag]
      : Option[ConnectionIO[Int]] =
    for {
      createTableDDL <- generateCreateDDL[T]()
    } yield Update0(createTableDDL, None).run

  override val hiveExecutorService: String => Option[ConnectionIO[Int]] = ddl =>
    Option(Update0(ddl, None).run)

  /*
  val hiveTestExecutorService: String => ConnectionIO[Option[Int]] = ddl => {
    val query: Query0[Int] = sql"""$ddl""".query[Int]
    println(query.toString)
    query.option
  }
 */

}
