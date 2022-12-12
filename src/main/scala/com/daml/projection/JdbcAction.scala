// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import com.typesafe.scalalogging.LazyLogging

import java.sql.{ PreparedStatement, SQLException }
import java.time.{ Instant, LocalDate, LocalDateTime }
import java.util.Optional
import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace
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

  /** Creates a [[Bind.Applied]] function that will set `value` at `pos` on a `PreparedStatement` argument. */
  final def apply(value: T, pos: Int): Bind.Applied = set(_, pos, value)
}

/**
 * Provides `Bind` values for [[ExecuteUpdate.bind[T](pos*]],[[ExecuteUpdate.bind[T](name*]], [[Binder.bind[T](pos*]]
 * and [[Binder.bind[T](name*]] methods.
 */
object Bind {
  type Used[-T] = Bind[_ >: T]

  private def mk[T <: AnyVal](f: (PreparedStatement, Int, T) => Unit): Bind[T] = new Bind[T] {
    override def set(ps: PreparedStatement, pos: Int, value: T) = f(ps, pos, value)
  }

  private def mkN[T >: Null <: AnyRef](f: (PreparedStatement, Int, T) => Unit): Bind[T] = new Bind[T] {
    override def set(ps: PreparedStatement, pos: Int, value: T) = {
      if (value eq null) ps.setNull(pos, java.sql.Types.NULL) else f(ps, pos, value)
    }
    // TODO SC would a specific thing from Types be better?
  }

  implicit val Boolean: Bind[Boolean] = mk(_.setBoolean(_, _))
  implicit val Byte: Bind[Byte] = mk(_.setByte(_, _))
  implicit val Short: Bind[Short] = mk(_.setShort(_, _))
  implicit val Int: Bind[Int] = mk(_.setInt(_, _))
  implicit val Long: Bind[Long] = mk(_.setLong(_, _))
  implicit val Float: Bind[Float] = mk(_.setFloat(_, _))
  implicit val Double: Bind[Double] = mk(_.setDouble(_, _))
  implicit val `scala BigDecimal`: Bind[BigDecimal] = mkN((ps, pos, x) => ps.setBigDecimal(pos, x.bigDecimal))
  implicit val BigDecimal: Bind[java.math.BigDecimal] = mkN(_.setBigDecimal(_, _))
  implicit val String: Bind[String] = mkN(_.setString(_, _))
  implicit val Date: Bind[java.sql.Date] = mkN(_.setDate(_, _))
  implicit val LocalDate: Bind[LocalDate] = mkN((ps, pos, x) => ps.setDate(pos, java.sql.Date.valueOf(x)))
  implicit val Timestamp: Bind[java.sql.Timestamp] = mkN(_.setTimestamp(_, _))
  implicit val LocalDateTime: Bind[LocalDateTime] =
    mkN((ps, pos, x) => ps.setTimestamp(pos, java.sql.Timestamp.valueOf(x)))
  implicit val Instant: Bind[Instant] =
    mkN((ps, pos, x) => ps.setTimestamp(pos, new java.sql.Timestamp(x.toEpochMilli)))
  implicit val Array: Bind[java.sql.Array] = mkN(_.setArray(_, _))

  implicit val Offset: Bind[Offset] = mkN((ps, pos, x) => ps.setString(pos, x.value))
  implicit val ProjectionId: Bind[ProjectionId] = mkN((ps, pos, x) => ps.setString(pos, x.value))

  implicit def Optional[A](implicit setter: Bind.Used[A]): Bind[Optional[A]] =
    mkN((ps, pos, x) => x.ifPresentOrElse(setter.set(ps, pos, _), () => ps.setNull(pos, java.sql.Types.NULL)))
  implicit def Option[A](implicit setter: Bind.Used[A]): Bind[Option[A]] =
    mkN((ps, pos, x) => x.map(setter.set(ps, pos, _)).getOrElse(ps.setNull(pos, java.sql.Types.NULL)))

  // Java API
  val Any: Bind[AnyRef] = mkN(_.setObject(_, _))

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
      ps.executeUpdate()
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
 * Binds fields of `R` to a `PreparedStatement`.
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
      if (rows.nonEmpty) ps.executeBatch().toList.sum
      else 0
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
