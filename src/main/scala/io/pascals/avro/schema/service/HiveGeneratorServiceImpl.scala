package io.pascals.avro.schema.service

import com.typesafe.scalalogging.Logger
import io.pascals.avro.schema.dialects.HiveDialect
import io.pascals.avro.schema.metadata.{ClassTypeMeta, MetadataExtractor, TypeMeta}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object HiveGeneratorServiceImpl extends ModelGeneratorService {

  private val log = Logger(getClass)

  /**
    * Generate data model for provided class
    *
    * @tparam T type for which to generate data model
    */
  override def generateCreateDDL[T: ClassTag : TypeTag]( ): Option[String] =
    Option(generate(getClassMeta[T]))

  /**
    * Alter data model for provided class metadata
    *
    * //@param c extracted class metadata
    */
  override def generateAlterDDL[T <: TypeMeta]( cls: T ):  Option[String] = cls match {
    case cls: ClassTypeMeta => Some(alterDataModel(cls))
    case _ => None
  }

  /**
    * Generate data model for provided class
    *
    * @param c extracted class metadata
    * @tparam T type for which generate data model
    */
  def generate[T: ClassTag : TypeTag]( c: ClassTypeMeta ): String = {
    val ct = implicitly[ClassTag[T]].runtimeClass
    log.info(s"Generating model for class: [${ct.getName}]")
    generateDataModel(HiveDialect.applyAnnotation(c))
  }

  private def alterDataModel( c: ClassTypeMeta ): String = {
    HiveDialect.alterDataModel(c, generateFieldsExpressions(c))
  }

  private def generateDataModel( c: ClassTypeMeta ): String = {
    HiveDialect.generateDataModel(c, generateFieldsExpressions(c))
  }

  private def generateFieldsExpressions( c: ClassTypeMeta ): Iterable[String] = {
    c
      .fields
      .withFilter(f => HiveDialect.generateColumn(f))
      .map(f => HiveDialect.generateClassFieldExpression(f))
  }

  private def getClassMeta[T: ClassTag : TypeTag]: ClassTypeMeta = MetadataExtractor.extractClassMeta[T]()


}
