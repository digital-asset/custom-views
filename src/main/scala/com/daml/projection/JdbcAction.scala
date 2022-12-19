// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import com.typesafe.scalalogging.LazyLogging

import java.sql.{ PreparedStatement, SQLException }
import java.time.{ Instant, LocalDate, LocalDateTime }
import java.util.Optional
import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace
import scala.util.chaining._
import scala.util.matching.Regex
import scala.jdk.FunctionConverters._
import java.util.{ function => juf }

/**
 * Captures the database action that can be executed by the [[javadsl.Projector]] or [[scaladsl.Projector]].
 */
@SerialVersionUID(1L)
@FunctionalInterface
trait JdbcAction extends java.io.Serializable {

  /**
   * Executes an SQL statement using the provided connection. Must return the number of rows affected.
   */
  def execute(con: java.sql.Connection): Int
}

/**
 * Rollbacks the current transaction and automatically starts a new transaction. (`autoCommit` must be set to false` on
 * the connection.)
 */
final case object Rollback extends JdbcAction {
  def execute(con: java.sql.Connection): Int = {
    con.rollback()
    0
  }
}

/**
 * Provides support for named parameters in SQL statements. See [[binder]] for creating a [[Binder]] that can be used in
 * [[UpdateMany]].
 */
object Sql {
  val BindPattern: Regex = """([^:]):([a-z]+[a-z_0-9]*)|([\?]{1})""".r

  /**
   * Creates a [[Binder]] from a `sql` statement.
   */
  def binder[R](sql: String): Binder[R] = parse(sql).binder[R]

  /**
   * Parses a sql statement into a [[Statement]].
   */
  def parse(sql: String): Statement = {
    val parameters = BindPattern.findAllMatchIn(sql).foldLeft(List.empty[Parameter]) { (res, m) =>
      val posAdded = Option(m.group(3)).fold(res) { _ =>
        res :+ PositionalParameter(res.size + 1)
      }
      Option(m.group(2)).fold(posAdded) { n =>
        posAdded :+ NamedParameter(n, posAdded.size + 1)
      }
    }
    val jdbcStr = BindPattern.replaceAllIn(sql, "$1?")
    Statement(jdbcStr, parameters, sql)
  }

  object Statement {

    /**
     * Creates a [[Statement]] from a parsed jdbcStr, parameters, and the original SQL string that was parsed using
     * [[Sql.parse]].
     */
    def apply(jdbcStr: String, parameters: List[Parameter], originalSql: String): Statement = {
      val namedParameters = parameters.collect { case n: NamedParameter => n }
      val positionalParameters = parameters.collect { case n: PositionalParameter => n }
      Statement(
        jdbcStr,
        originalSql,
        namedParameters.map(n => (n.name, n)).toMap,
        positionalParameters,
        namedParameters.map(_.name))
    }
  }

  /**
   * Represents a SQL statement and its bind parameters.
   */
  final case class Statement(
      jdbcStr: String,
      originalSql: String,
      namedParameters: Map[String, Parameter],
      positionalParameters: List[Parameter],
      parameterNames: List[String],
      binds: Map[Int, Bind.Applied] = Map.empty[Int, Bind.Applied]
  ) {
    def parametersSize = namedParameters.size + positionalParameters.size
    def binder[R] = Binder[R](this)
    def isBound = parametersSize == binds.size

    /**
     * Bind an argument to a parameter by position in the SQL query.
     */
    def bind[T: Bind.Used](pos: Int, arg: T): Statement =
      copy(binds = binds + (pos -> bindValue(pos, arg)))

    /**
     * Bind an argument to a named parameter. Parameters start with a colon ':' followed by a letter. Numbers,
     * underscore and lowercase letters are allowed characters in a named parameter.
     */
    def bind[T: Bind.Used](name: String, arg: T): Statement = {
      require(!name.startsWith(":"), s"cannot bind to name $name, must not start with ':'.")
      val delimiter = ", "
      namedParameters.get(name).map { namedParameter =>
        require(!binds.contains(namedParameter.position), s"cannot bind to the same name '$name' more than once")
        copy(binds = binds + (namedParameter.position -> bindValue(namedParameter.position, arg)))
      }.getOrElse {
        if (!namedParameters.isEmpty) throw new IllegalArgumentException(
          s"Cannot bind '$name'. sql statement:\n${originalSql}'\nbind parameters:\n${namedParameters.values.mkString(delimiter)}")
        else throw new IllegalArgumentException(
          s"Cannot bind '$name', no bind parameters in sql statement:\n${originalSql}")
      }
    }

    def bindValue[T: Bind.Used](pos: Int, arg: T): Bind.Applied = {
      require(pos > 0, "bind position must be >= 1")
      require(
        pos <= parametersSize,
        s"bind position '$pos', out of bounds for ${parametersSize} bind parameters"
      )
      require(!binds.contains(pos), s"cannot bind to the same position '$pos' more than once")
      val setter = implicitly[Bind.Used[T]]
      setter.apply(arg, pos)
    }

    def bindValue[T: Bind.Used](name: String, arg: T): Bind.Applied = {
      val setter = implicitly[Bind.Used[T]]

      val delimiter = ", "
      namedParameters.get(name).map { namedParameter =>
        require(!binds.contains(namedParameter.position), s"cannot bind to the same name '$name' more than once")
        setter.apply(arg, namedParameter.position)
      }.getOrElse {
        if (!namedParameters.isEmpty) throw new IllegalArgumentException(
          s"Cannot bind '$name'. sql statement:\n${originalSql}'\nbind parameters:\n${namedParameters.values.mkString(delimiter)}")
        else throw new IllegalArgumentException(
          s"Cannot bind '$name', no bind parameters in sql statement:\n${originalSql}")
      }
    }
  }

  /** A SQL [[Statement]] parameter */
  sealed trait Parameter {
    def position: Int
  }

  /**
   * A Named SQL [[Statement]] parameter, specified in a SQL statement. Parameters start with a colon ':' followed by a
   * letter. Numbers, underscore and lowercase letters are allowed characters in a named parameter.
   */
  final case class NamedParameter(name: String, position: Int) extends Parameter

  /** A Positional SQL [[Statement]] parameter, specified in a SQL statement with `?`. */
  final case class PositionalParameter(position: Int) extends Parameter
}

/**
 * Binds a value of `T` to a [[java.sql.PreparedStatement]].
 */
@FunctionalInterface
trait Bind[-T] {

  /** Sets the value on the [[java.sql.PreparedStatement]] */
  def set(ps: PreparedStatement, pos: Int, value: T): Unit

  /**
   * Creates a [[Bind.Applied]] function that will set `value` at `pos` on a [[java.sql.PreparedStatement]] argument.
   */
  final def apply(value: T, pos: Int): Bind.Applied = set(_, pos, value)

  /**
   * Creates a [[Bind]] from this [[Bind]] by converting `U` to `T` using `f`.
   */
  def contramap[U](f: U => T) = new Bind[U] {
    override def set(ps: PreparedStatement, pos: Int, value: U) = {
      Bind.this.set(ps, pos, f(value))
    }
    override def nullSqlType: Int = Bind.this.nullSqlType
  }

  /**
   * The [[java.sql.Types]] code that should be used when invoking `setNull` on [[java.sql.PreparedStatement]]. Default
   * in this trait is set to java.sql.Types.NULL, override if a more specific code is needed.
   */
  def nullSqlType: Int = java.sql.Types.NULL

  /**
   * Return a new Bind with the [[java.sql.Types]] code modified, that should be used when invoking `setNull` on
   * [[java.sql.PreparedStatement]].
   */
  def withNullSqlType(sqlType: Int): Bind[T] = new Bind[T] {
    override def nullSqlType: Int = sqlType
    override def set(ps: PreparedStatement, pos: Int, value: T): Unit = {
      Bind.this.set(ps, pos, value)
    }
  }
}

/**
 * Provides `Bind` values for [[ExecuteUpdate.bind[T](pos*]],[[ExecuteUpdate.bind[T](name*]], [[Binder.bind[T](pos*]]
 * and [[Binder.bind[T](name*]] methods.
 */
object Bind {
  type Used[-T] = Bind[_ >: T]

  private def mk[T <: AnyVal](f: (PreparedStatement, Int, T) => Unit, _nullSqlType: Int): Bind[T] = new Bind[T] {
    override def set(ps: PreparedStatement, pos: Int, value: T) = f(ps, pos, value)
    override def nullSqlType = _nullSqlType
  }

  private def mkN[T >: Null <: AnyRef](f: (PreparedStatement, Int, T) => Unit, _nullSqlType: Int): Bind[T] =
    new Bind[T] {
      override def nullSqlType = _nullSqlType
      override def set(ps: PreparedStatement, pos: Int, value: T) = {
        if (value eq null) ps.setNull(pos, _nullSqlType)
        else f(ps, pos, value)
      }
    }

  implicit val _Boolean: Bind[Boolean] = mk(_.setBoolean(_, _), java.sql.Types.BOOLEAN)
  implicit val _Byte: Bind[Byte] = mk(_.setByte(_, _), java.sql.Types.SMALLINT)
  implicit val _Short: Bind[Short] = mk(_.setShort(_, _), java.sql.Types.SMALLINT)
  implicit val _Int: Bind[Int] = mk(_.setInt(_, _), java.sql.Types.INTEGER)
  implicit val _Long: Bind[Long] = mk(_.setLong(_, _), java.sql.Types.BIGINT)
  implicit val _Float: Bind[Float] = mk(_.setFloat(_, _), java.sql.Types.FLOAT)
  implicit val _Double: Bind[Double] = mk(_.setDouble(_, _), java.sql.Types.DOUBLE)
  implicit val _BigDecimal: Bind[BigDecimal] =
    mkN((ps, pos, x) => ps.setBigDecimal(pos, x.bigDecimal), java.sql.Types.DECIMAL)

  implicit val Boolean: Bind[java.lang.Boolean] = _Boolean.contramap(_.booleanValue)
  implicit val Byte: Bind[java.lang.Byte] = _Byte.contramap(_.toByte)
  implicit val Short: Bind[java.lang.Short] = _Short.contramap(_.toShort)
  implicit val Int: Bind[java.lang.Integer] = _Int.contramap(_.toInt)
  implicit val Long: Bind[java.lang.Long] = _Long.contramap(_.toLong)
  implicit val Float: Bind[java.lang.Float] = _Float.contramap(_.toFloat)
  implicit val Double: Bind[java.lang.Double] = _Double.contramap(_.toDouble)
  implicit val BigDecimal: Bind[java.math.BigDecimal] = mkN(_.setBigDecimal(_, _), java.sql.Types.DECIMAL)

  implicit val String: Bind[String] = mkN(_.setString(_, _), java.sql.Types.VARCHAR)
  implicit val Date: Bind[java.sql.Date] = mkN(_.setDate(_, _), java.sql.Types.DATE)
  implicit val LocalDate: Bind[LocalDate] = Date.contramap(java.sql.Date.valueOf(_))
  implicit val Timestamp: Bind[java.sql.Timestamp] = mkN(_.setTimestamp(_, _), java.sql.Types.TIMESTAMP)
  implicit val LocalDateTime: Bind[LocalDateTime] = Timestamp.contramap(java.sql.Timestamp.valueOf(_))

  implicit val Instant: Bind[Instant] = Timestamp.contramap(x => new java.sql.Timestamp(x.toEpochMilli))
  implicit val Array: Bind[java.sql.Array] = mkN(_.setArray(_, _), java.sql.Types.ARRAY)

  implicit val Offset: Bind[Offset] = String.contramap(_.value)
  implicit val ProjectionId: Bind[ProjectionId] = String.contramap(_.value)

  implicit def Optional[A](implicit setter: Bind.Used[A]): Bind[Optional[A]] =
    mkN(
      (ps, pos, x) => x.ifPresentOrElse(setter.set(ps, pos, _), () => ps.setNull(pos, setter.nullSqlType)),
      setter.nullSqlType)
  implicit def Option[A](implicit setter: Bind.Used[A]): Bind[Option[A]] = {
    mkN(
      (ps, pos, x) => x.map(setter.set(ps, pos, _)).getOrElse(ps.setNull(pos, setter.nullSqlType)),
      setter.nullSqlType)
  }

  // Java API
  val Any: Bind[AnyRef] = mkN(_.setObject(_, _), java.sql.Types.NULL)

  type Applied = PreparedStatement => Unit
}

/**
 * Creates a prepared statement from a `sql` statement. Use the [[ExecuteUpdate]] to insert, update or delete rows in
 * the projection table.
 */
object ExecuteUpdate {

  /**
   * See [[create]]
   */
  def apply(sql: String): ExecuteUpdate = ExecuteUpdate(Sql.parse(sql))

  /**
   * Creates an ExecuteUpdate from a `sql` statement. The `sql` statement can contain positional and / or named
   * parameters. Positional parameters are marked with '?' in the `sql` statement. Named parameters follow the
   * [[Sql.BindPattern]] pattern:
   *
   *   1. Must start with ':', followed by at least one letter.
   *   1. Underscore ('_'), letters and digits are allowed characters.
   *
   * Named parameters are internally converted to positional parameters. If both named and positional parameters are
   * used, binding by position must take account of the positions of the named parameters.
   */
  def create(sql: String): ExecuteUpdate = ExecuteUpdate(Sql.parse(sql))
}

/**
 * Creates a prepared statement from the `sql` and `setter` arguments and calls executeUpdate on it. Use this action to
 * insert, update or delete rows in the projection table.
 */
final case class ExecuteUpdate(sql: Sql.Statement)
    extends JdbcAction
    with LazyLogging {
  private val binds = sql.binds.values.toList
  def execute(con: java.sql.Connection): Int = {
    val ps = con.prepareStatement(sql.jdbcStr)
    try {
      binds.foreach(bind => bind(ps))
      logger.trace(s"Start executeUpdate: ${sql.jdbcStr}")
      ps.executeUpdate().tap { rows =>
        logger.trace(s"Success executeUpdate: $rows effected")
      }
    } catch {
      case t: Throwable =>
        logger.debug(s"""Failed ExecuteUpdate, sql: '$sql'""", t)
        throw t
    } finally {
      ps.close()
    }
  }

  /**
   * Bind an argument to a parameter by position in the SQL query.
   */
  def bind[T: Bind.Used](pos: Int, arg: T): ExecuteUpdate = copy(sql = sql.bind(pos, arg))

  /**
   * Bind an argument to a named parameter. In the SQL query, parameters start with a colon ':' numbers, underscore and
   * lowercase letters are allowed characters in a named parameter. Provide `name` without ':' to this method.
   */
  def bind[T: Bind.Used](name: String, arg: T): ExecuteUpdate = copy(sql = sql.bind(name, arg))
}

/**
 * Thrown when an [[ExecuteUpdate]] could not be bound using [[ExecuteUpdate.bind[T](pos*]], wrong index, or the bind
 * parameter name could not be found using [[ExecuteUpdate.bind[T](name*]].
 */
final case class SqlBindException(msg: String) extends Exception(msg) with NoStackTrace

object Binder {
  def create[R](sql: Sql.Statement): Binder[R] = Binder[R](sql)
}

/**
 * Binds fields of `R` to a [[java.sql.PreparedStatement]].
 * @tparam R
 *   the type from which [[Bind]]s are created.
 */
final case class Binder[R](
    sql: Sql.Statement,
    rowBinds: List[R => PreparedStatement => Unit] = List.empty[R => PreparedStatement => Unit]) {

  private def bind[T: Bind.Used](pos: Int, field: R => T): Binder[R] = {
    def setter(row: R): PreparedStatement => Unit = {
      sql.bindValue(pos, field(row))
    }
    copy(rowBinds = rowBinds :+ setter)
  }

  private def bind[T: Bind.Used](name: String, field: R => T): Binder[R] = {
    require(!name.startsWith(":"), s"cannot bind to name $name, must not start with ':'.")
    def setter(row: R): PreparedStatement => Unit = {
      sql.bindValue(name, field(row))
    }
    copy(rowBinds = rowBinds :+ setter)
  }

  /**
   * Bind an argument to a parameter by position in the SQL query. The `field` function must extract a field from `R`
   * that will be used as the argument for the parameter.
   */
  def bind[T: Bind.Used](pos: Int, field: juf.Function[R, T]): Binder[R] =
    bind(pos, field.asScala)

  /**
   * Bind an argument to a named parameter. In the SQL query, parameters start with a colon ':' numbers, underscore and
   * lowercase letters are allowed characters in a named parameter. Provide `name` without ':' to this method. The
   * `field` function must extract a field from `R` that will be used as the argument for the parameter.
   */
  def bind[T: Bind.Used](name: String, field: juf.Function[R, T]): Binder[R] =
    bind(name, field.asScala)
}

/**
 * Executes and reuses a prepared SQL statement for the `rows` supplied, converting the `rows` into bind parameters
 * using the provided `write`.
 */
final case class ExecuteUpdateMany[R](sql: Sql.Statement, rows: Seq[R], binder: Binder[R]) extends JdbcAction
    with LazyLogging {
  def execute(con: java.sql.Connection): Int = {
    val ps = con.prepareStatement(sql.jdbcStr)
    try {
      rows.foreach { row =>
        ps.clearParameters()

        binder.rowBinds.foreach(set => set(row)(ps))
        ps.addBatch()
      }
      if (rows.nonEmpty) {
        logger.trace(s"Start executeBatch")
        ps.executeBatch().toList.sum.tap { rows =>
          logger.trace(s"Success executeBatch: $rows effected")
        }
      } else 0
    } catch {
      case t: Throwable =>
        logger.error(s"""Failed ExecuteUpdateMany.""", t)
        throw t
    } finally {
      ps.close()
    }
  }
}

/**
 * An action that is immediately committed.
 * @param action
 *   the action to commit
 */
final case class CommittedAction(action: JdbcAction) extends JdbcAction {
  def execute(con: java.sql.Connection): Int = {
    val res = action.execute(con)
    con.commit()
    res
  }
}

/**
 * Commits the current transaction and automatically starts a new transaction, if autoCommit is set to false on the
 * connection.
 */
final case object Commit extends JdbcAction {
  def execute(con: java.sql.Connection): Int = {
    con.commit()
    0
  }
}

/**
 * Executes the `action`, executes the action provided by `handler` if the `action` failed.
 * @param action
 *   the action to execute
 * @param handler
 *   the handler function that executes an action in case of a SQL Exception
 */
final case class HandleError(action: JdbcAction, handler: java.sql.SQLException => JdbcAction) extends JdbcAction {
  def execute(con: java.sql.Connection): Int = {
    try {
      action.execute(con)
    } catch {
      case e: SQLException => handler(e).execute(con)
    }
  }
}

object UpdateMany {
  def create[R](binder: Binder[R]): BatchRows[R, JdbcAction] = {
    rows => ExecuteUpdateMany(binder.sql, rows.asScala.toList, binder)
  }
  def apply[R](binder: Binder[R]): Seq[R] => JdbcAction =
    rows => ExecuteUpdateMany(binder.sql, rows, binder)
}
