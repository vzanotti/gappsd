#!/usr/bin/python
#
# Copyright (C) 2008 Polytechnique.org
# Author: Vincent Zanotti (vincent.zanotti@polytechnique.org)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Offers an simplified interface to the SQL database: once it has been
initialized with the gappsd' credentials, it offers an easy-to-use interface
for insertion, data update, and generic data retrieving."""

import MySQLdb
import MySQLdb.cursors as cursors
import warnings

from . import logger
from .logger import PermanentError, TransientError

class SQLTransientError(TransientError):
  """Generic exception for transient errors (eg. connection lost)"""
  pass

class SQLPermanentError(PermanentError):
  """Generic exception for permanent errors (eg. SQL syntax error)"""
  pass


class SQL(object):
  """Offers a simplified interface to the MySQL database.
  SQL queries offered are: UPDATE (Update), INSERT (Insert), and any other query
  that fit in the model Query (returns the resulting data) or Execute (returns
  the number of line of the result).

  Example usage:
    sql = SQL(config)
    sql.Query("SELECT * FROM foo WHERE bar = %s", (qux,))
    sql.Close()
  """

  def __init__(self, config):
    """Initializes the SQL object, and opens a connection to the database."""

    self._host = config.get_string("mysql.hostname")
    self._user = config.get_string("mysql.username")
    self._pass = config.get_string("mysql.password")
    self._db = config.get_string("mysql.database")

    self._connection = None

  # Operations on underlying connection.
  def Open(self):
    """Opens the connection to the database. If there is already an opened
    connection, does nothing."""

    if self._connection == None:
      try:
        self._connection = MySQLdb.connect(
          host=self._host, user=self._user, passwd=self._pass, db=self._db,
          charset='utf8', use_unicode=True)
      except MySQLdb.Error, message:
        raise SQLTransientError("Error: %s" % message)

  def Close(self):
    """Closes the connection to the database, if one is opened."""
    if not self._connection == None:
      self._connection.close()
      self._connection = None

  # Internal SQL query interface.
  def __Query(self, cursor_class, query, args, fetch=False):
    """Executes the query using the @p args, and resulting in a cursor of the
    @p class. It also catches MySQL exception, and re-raise them in two forms:

    * SQLTransientError for potentially transient errors (eg. connection lost).
      In this case, the query should be retried.
    * SQLPermanentError for non-transient errors and warnings (eg. syntax error,
      data error, ...).
    """

    if self._connection == None:
      self.Open()
    cursor = self._connection.cursor(cursor_class)

    try:
      results = cursor.execute(query, args)
      data = None if not fetch else cursor.fetchall()
    except MySQLdb.DataError, message:
      raise SQLPermanentError("DataError: %s" % message)
    except MySQLdb.IntegrityError, message:
      raise SQLPermanentError("IntegrityError: %s" % message)
    except MySQLdb.ProgrammingError, message:
      raise SQLPermanentError("ProgrammingError: %s" % message)
    except MySQLdb.Warning, message:
      logger.critical("SQL Warning: %s" % message)
      return (False, None)
    except MySQLdb.Error, message:
      raise SQLTransientError("Error: %s" % message)

    return (results, data)

  # Data manipulations.
  def Update(self, table, values, where):
    """Updates the @p table by setting new values for keys contained in @p
    values, and using the @p where dictionary to select entries.
    Cf. __Query for information on raised exceptions."""
    args = list(values.values()) + list(where.values())
    query = "UPDATE %s SET " % table + \
      ", ".join(["%s = %%s" % field for field in values]) + " WHERE " + \
      " AND ".join(["%s = %%s" % field for field in where])
    return self.Execute(query, args)

  def Insert(self, table, values):
    """Inserts a new record in the @p table, using the @p values dictionary
    as data source.
    Cf. __Query for information on raised exceptions."""
    args = list(values.values());
    query = "INSERT INTO %s SET " % table + \
      ", ".join(["%s = %%s" % field for field in values])
    return self.Execute(query, args)

  def Execute(self, query, args=()):
    """Queries the SQL database using the @p query filled with @p args.
    The query returns the number of rows.
    Cf. __Query for information on raised exceptions."""
    return self.__Query(cursors.Cursor, query, args)[0]

  def Query(self, query, args=()):
    """Queries the SQL database using the @p query filled with @p args.
    The query returns a tuple of dictionaries, containing the result.
    Cf. __Query for information on raised exceptions."""
    return self.__Query(cursors.DictCursor, query, args, fetch=True)[1]

# Initialization: transforms MySQL warnings in errors.
warnings.simplefilter("error", MySQLdb.Warning)