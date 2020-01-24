package io.pascals.avro.schema.metadata

import scala.reflect.{ClassTag, NameTransformer}
import scala.reflect.runtime.{universe => ru}
import ru._

object MetadataExtractor {

  /*
   * The metadata extractor uses runtime reflection on Scala ADTs and builds ClassTypeMeta objects using the mappings/
   * metadata defined in the metadata package object. Note that this works for product types only and not for sum types
   * because Hive does not allow creation of tables without schema definition.
   *
   * */

  private def mirror: ru.Mirror =
    ru.runtimeMirror(Thread.currentThread().getContextClassLoader)

  def localTypeOf[T: ru.TypeTag](): ru.Type = this.synchronized {
    val tag: ru.TypeTag[T] = implicitly[ru.TypeTag[T]]
    tag.in(mirror).tpe.dealias
  }

  def product(tpe: ru.Type): Boolean = tpe <:< localTypeOf[Product]()

  /*
   * For all intents and purposes, this is the entry point to what we are trying to do. i.e Extract information out of
   * scala ADT into a previously defined `ClassTypeMeta` object, which can later be used to generate dialects that one
   * provides implementations for.
   *
   * */
  def extractClassMeta[T: ru.TypeTag: ClassTag](): ClassTypeMeta = {
    val tpe = localTypeOf[T]()
    tpe match {
      case t if product(t) => extractClassMeta(t)
      case other =>
        throw new UnsupportedOperationException(
          s"Metadata for type $other is not supported"
        )
    }
  }

  private def extractClassMeta(tpe: ru.Type): ClassTypeMeta = {
    val cls = mirror.runtimeClass(tpe)
    ClassTypeMeta(
      packageName = cls.getPackage.getName,
      originalTypeName = cls.getSimpleName,
      typeName = cls.getSimpleName,
      annotations = extractAnnotations(tpe.typeSymbol),
      fields = extractClassFields(tpe)
    )
  }

  private def extractClassFields(tpe: ru.Type): Iterable[ClassFieldMeta] = {
    val paramList: List[ru.Symbol] =
      tpe.typeSymbol.asClass.primaryConstructor.typeSignature.paramLists.head
    paramList.map { symbol =>
      ClassFieldMeta(
        originalFieldName = NameTransformer.decode(symbol.name.toString),
        fieldName = NameTransformer.decode(symbol.name.toString),
        fieldType = extractTypeMeta(symbol.typeSignature),
        annotations = extractAnnotations(symbol)
      )
    }
  }

  private def extractAnnotations(symbol: ru.Symbol): Seq[AnnotationMeta] = {
    symbol.annotations
      .map(
        a =>
          AnnotationMeta(
            name = a.tree.tpe.toString,
            attributes = {
              val params = a.tree.tpe.decls
                .find(_.name.toString == "<init>")
                .get
                .asMethod
                .paramLists
                .flatten
                .map(f => f.name.toString)
              val values = a.tree.children.tail
              (params zip values)
                .map(
                  pv =>
                    AnnotationAttribute(
                      name = pv._1,
                      value = pv._2.toString.stripPrefix("\"").stripSuffix("\"")
                    )
                )
            }
          )
      )
  }

  private def extractTypeMeta(tpe: ru.Type): TypeMeta = tpe match {
    case t if t <:< localTypeOf[Option[_]] =>
      val TypeRef(_, _, Seq(optType)) = t
      extractTypeMeta(optType)
    case t if t <:< localTypeOf[Array[Byte]] => BinaryType
    case t if t <:< localTypeOf[Array[_]] =>
      val TypeRef(_, _, Seq(elementType)) = t
      SeqTypeMeta(extractTypeMeta(elementType))
    case t if t <:< localTypeOf[Seq[_]] =>
      val TypeRef(_, _, Seq(elementType)) = t
      SeqTypeMeta(extractTypeMeta(elementType))
    case t if t <:< localTypeOf[Map[_, _]] =>
      val TypeRef(_, _, Seq(keyType, valueType)) = t
      MapTypeMeta(extractTypeMeta(keyType), extractTypeMeta(valueType))
    case t if t <:< localTypeOf[Iterable[_]] =>
      val TypeRef(_, _, Seq(elementType)) = t
      SeqTypeMeta(extractTypeMeta(elementType))
    case t if t <:< localTypeOf[String]               => StringType
    case t if t <:< localTypeOf[java.sql.Timestamp]   => TimestampType
    case t if t <:< localTypeOf[java.util.Date]       => DateType
    case t if t <:< localTypeOf[java.sql.Date]        => DateType
    case t if t <:< localTypeOf[BigDecimal]           => BigDecimalType
    case t if t <:< localTypeOf[java.math.BigDecimal] => BigDecimalType
    case t if t <:< localTypeOf[java.math.BigInteger] => BigIntegerType
    case t if t <:< localTypeOf[scala.math.BigInt]    => BigIntegerType
    case t if t <:< localTypeOf[java.lang.Integer]    => IntegerType
    case t if t <:< localTypeOf[java.lang.Long]       => LongType
    case t if t <:< localTypeOf[java.lang.Double]     => DoubleType
    case t if t <:< localTypeOf[java.lang.Float]      => FloatType
    case t if t <:< localTypeOf[java.lang.Short]      => ShortType
    case t if t <:< localTypeOf[java.lang.Byte]       => ByteType
    case t if t <:< localTypeOf[java.lang.Boolean]    => BooleanType
    case t if t <:< definitions.IntTpe                => IntegerType
    case t if t <:< definitions.LongTpe               => LongType
    case t if t <:< definitions.DoubleTpe             => DoubleType
    case t if t <:< definitions.FloatTpe              => FloatType
    case t if t <:< definitions.ShortTpe              => ShortType
    case t if t <:< definitions.ByteTpe               => ByteType
    case t if t <:< definitions.BooleanTpe            => BooleanType
    case t if product(t)                              => extractClassMeta(t)
    case other =>
      throw new UnsupportedOperationException(
        s"MetaData for type $other is not supported"
      )
  }

}
