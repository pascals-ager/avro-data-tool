package io.github.pascals.avro.schema.annotations

import scala.annotation.StaticAnnotation

package object hive {

  /**
    * Generate Hive Column
    *
    * @param name custom column name
    */
  final class column(val name: String) extends StaticAnnotation

  /**
    * Generate HIVE Table
    *
    * @param name custom table name
    */
  final class hiveTable(val name: String) extends StaticAnnotation

  /**
    * Generated Hive DLL should be EXTERNAL table
    *
    * @param location location of data
    */
  final class hiveExternalTable(val location: String) extends StaticAnnotation

  /**
    * Convert table name and column names to underscore
    *
    * Example personName -> person_name
    */
  final class underscore extends StaticAnnotation

  /**
    * Marking column as Hive partition
    *
    * @param order order of partitioning, without providing order value partitioning order is the same as order of class fields
    */
  final class hivePartitionColumn(val order: Int = 0) extends StaticAnnotation

  /**
    * Marking column as Hive bucket
    *
    * @param buckets number of buckets that the column can be clustered into
    */
  final class hiveBucket(val buckets: Int = 1) extends StaticAnnotation

  /**
    * STORED AS X clause
    *
    * @param format format of data e.g. PARQUET
    */
  final class hiveStoredAs(val format: String) extends StaticAnnotation

  /**
    * Hive TBLPROPERTIES section
    *
    * @param key key of table property
    * @param value value of table property
    */
  final class hiveTableProperty(val key: String, val value: String)
      extends StaticAnnotation

}
