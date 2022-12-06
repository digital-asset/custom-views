// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.projection

import com.typesafe.scalalogging.LazyLogging

import java.sql.SQLException
import java.util.Optional
import scala.jdk.CollectionConverters._
import scala.util.control.NoStackTrace
import scala.util.matching.Regex
import scala.jdk.FunctionConverters._
import java.{ util => ju }
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
      setters: Map[Int, Setter] = Map.empty[Int, Setter]
  ) {
    def totalParameters = namedParameters.size + positionalParameters.size
    def binder[R] = Binder[R](this)

    /**
     * Bind an argument to a parameter by position in the SQL query.
     */
    def bind[T](pos: Int, arg: T): Statement = {
      copy(setters = setters + (pos -> setter(pos, arg)))
    }

    def setter[T](pos: Int, arg: T): Setter = {
      require(pos > 0, "bind position must be >= 1")
      require(
        pos <= totalParameters,
        s"bind position '$pos', out of bounds for ${totalParameters} bind parameters"
      )
      require(!setters.contains(pos), s"cannot bind to the same position '$pos' more than once")
      BindValue(arg, pos)
    }

    def setter[T](name: String, arg: T): Setter = {
      val delimiter = ", "
      namedParameters.get(name).map { namedParameter =>
        require(!setters.contains(namedParameter.position), s"cannot bind to the same name '$name' more than once")
        BindValue(arg, namedParameter.position)
      }.getOrElse {
        if (!namedParameters.isEmpty) throw new IllegalArgumentException(
          s"Cannot bind '$name'. sql statement:\n${originalSql}'\nbind parameters:\n${namedParameters.values.mkString(delimiter)}")
        else throw new IllegalArgumentException(
          s"Cannot bind '$name', no bind parameters in sql statement:\n${originalSql}")
      }
    }

    /**
     * Bind an argument to a named parameter. Parameters start with a colon ':' followed by a letter. Numbers,
     * underscore and lowercase letters are allowed characters in a named parameter.
     */
    def bind[T](name: String, arg: T): Statement = {
      val delimiter = ", "
      namedParameters.get(name).map { namedParameter =>
        require(!setters.contains(namedParameter.position), s"cannot bind to the same name '$name' more than once")
        copy(setters = setters + (namedParameter.position -> BindValue(arg, namedParameter.position)))
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
 * Binds a value to a position in the PreparedStatement.
 */
final case class BindValue[T](value: T, pos: Int) extends Setter {
  def set(ps: java.sql.PreparedStatement) = {
    value match {
      case x: Boolean                   => ps.setBoolean(pos, x)
      case x: Byte                      => ps.setByte(pos, x)
      case x: Short                     => ps.setShort(pos, x)
      case x: Int                       => ps.setInt(pos, x)
      case x: Long                      => ps.setLong(pos, x)
      case x: Float                     => ps.setFloat(pos, x)
      case x: Double                    => ps.setDouble(pos, x)
      case x: BigDecimal                => ps.setBigDecimal(pos, x.bigDecimal)
      case x: java.math.BigDecimal      => ps.setBigDecimal(pos, x)
      case x: String                    => ps.setString(pos, x)
      case x: java.sql.Date             => ps.setDate(pos, x)
      case x: java.sql.Timestamp        => ps.setTimestamp(pos, x)
      case x: java.sql.Array            => ps.setArray(pos, x)
      case null                         => ps.setNull(pos, java.sql.Types.NULL)
      case x: Optional[_] if x.isEmpty  => ps.setNull(pos, java.sql.Types.NULL)
      case x: Optional[_] if !x.isEmpty => ps.setObject(pos, x.get())
      // TODO fix in typeclass solution
      case x: Option[_] if x.isEmpty => ps.setNull(pos, java.sql.Types.NULL)
      case Some(x)                   => ps.setObject(pos, x)
      case x                         => ps.setObject(pos, x)
    }
  }
}

/** Sets a bind value on a PreparedStatement */
@SerialVersionUID(1L)
@FunctionalInterface
trait Setter extends java.io.Serializable {

  /** Sets a bind value on `ps` */
  def set(ps: java.sql.PreparedStatement): Unit
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
  private val _setters = sql.setters.values.toList

  def execute(con: java.sql.Connection): Int = {
    val ps = con.prepareStatement(sql.jdbcStr)
    try {
      _setters.foreach(_.set(ps))
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
  def bind[T](pos: Int, arg: T): ExecuteUpdate = copy(sql = sql.bind(pos, arg))

  /**
   * Bind an argument to a named parameter. Parameters start with a colon ':' numbers, underscore and lowercase letters
   * are allowed characters in a named parameter.
   */
  def bind[T](name: String, arg: T): ExecuteUpdate = copy(sql = sql.bind(name, arg))
}

/**
 * Thrown when an [[ExecuteUpdate]] could not be bound using [[ExecuteUpdate.bind[T](pos*]], wrong index, or the bind
 * parameter name could not be found using [[ExecuteUpdate.bind[T](name*]].
 */
final case class SqlBindException(msg: String) extends Exception(msg) with NoStackTrace

object Binder {
  def create[R](sql: Sql.Statement): Binder[R] = Binder[R](sql)
  def create[R](sql: Sql.Statement, binder: juf.Function[R, Setter]): Binder[R] = Binder[R](sql, List(binder.asScala))
  def create[R](sql: Sql.Statement, binders: ju.List[juf.Function[R, Setter]]): Binder[R] =
    Binder[R](sql, binders.asScala.map(_.asScala).toList)
}

/**
 * Transforms an `R` into a list of functions that creates [[Setter]]s from `R`.
 * @tparam R
 *   the type from which Setters are mapped.
 */
final case class Binder[R](sql: Sql.Statement, setterCreators: List[R => Setter] = List.empty[R => Setter]) {
  private def bind(r: R => Setter): Binder[R] = {
    copy(setterCreators = setterCreators :+ r)
  }

  private def bind[T](pos: Int, field: R => T): Binder[R] = {
    def setter(row: R) = sql.setter(pos, field(row))
    copy(setterCreators = setterCreators :+ setter)
  }

  private def bind[T](name: String, field: R => T): Binder[R] = {
    def setter(row: R) = sql.setter(name, field(row))
    copy(setterCreators = setterCreators :+ setter)
  }

  def bind[T](pos: Int, field: juf.Function[R, T]): Binder[R] =
    bind(pos, field.asScala)

  def bind[T](name: String, field: juf.Function[R, T]): Binder[R] =
    bind(name, field.asScala)

  def bind(r: juf.Function[R, Setter]): Binder[R] = bind(r.asScala)
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
        binder.setterCreators.foreach(b => b(row).set(ps))
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
