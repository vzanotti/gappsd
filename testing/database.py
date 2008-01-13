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

import gappsd.database as database
import unittest

class MockSQL(object):
  """Defines a mock SQL connection that registers SQL queries, and allows
  to set predefined results.

  For SQL.Update():
    update_table, update_values, and update_where contain the last query.
    update_result contains the result of the next update (None or an exception)
  For SQL.Insert():
    insert_table, and insert_where contain the last query.
    insert_result contains the result of the next insert (None or an exception)
  For SQL.Execute():
    execute_query, and execute_args contains the last query
    execute_result contains the result of the next query (None or an exception)
  For SQL.Query():
    query_query, and query_args contain the last query
    query_result contains the result of the next query (None, or an exception,
      or a sequence of dictionaries)
  """

  def __init__(self):
    self.update_table = None
    self.update_values = None
    self.update_where = None
    self.insert_table = None
    self.insert_values = None
    self.execute_query = None
    self.execute_args = None
    self.execute_result = None
    self.query_query = None
    self.query_args = None
    self.query_result = None

  def Update(self, table, values, where=None):
    self.update_table = table
    self.update_values = values
    self.update_where = where

  def Insert(self, table, values):
    self.insert_table = table
    self.insert_values = values

  def Execute(self, query, args=None):
    self.execute_query = query
    self.execute_args = args
    return self.result(self.execute_result)

  def Query(self, query, args=None):
    self.query_query = query
    self.query_args = args
    return self.result(self.query_result)

  def result(self, result):
    try:
      if issubclass(result, Exception):
        raise result
    except:
      return result

# TODO(zanotti): Add unittest for the SQL object.
