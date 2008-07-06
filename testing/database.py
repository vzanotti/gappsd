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

import MySQLdb, MySQLdb.connections, MySQLdb.cursors
import gappsd.database as database
import gappsd.logger as logger
import testing.config
import mox, unittest

class TestSQL(mox.MoxTestBase):
  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.config = testing.config.MockConfig()
    self.connection = self.mox.CreateMock(MySQLdb.connections.Connection)
    self.mock_cursor = self.mox.CreateMock(MySQLdb.cursors.Cursor)
    self.sql = database.SQL(self.config)

    self.mox.StubOutWithMock(self.sql, '_SQL__Query')
    self.sql._connection = self.connection

  def testOpenNormal(self):
    self.mox.StubOutWithMock(MySQLdb, 'connect')
    MySQLdb.connect(charset='utf8',
                    db=mox.IgnoreArg(),
                    host=mox.IgnoreArg(),
                    passwd=mox.IgnoreArg(),
                    use_unicode=True,
                    user=mox.IgnoreArg())
    self.mox.ReplayAll()

    self.sql._connection = None
    self.sql.Open()

  def testOpenFailed(self):
    self.mox.StubOutWithMock(MySQLdb, 'connect')
    MySQLdb.connect(charset='utf8',
                    db=mox.IgnoreArg(),
                    host=mox.IgnoreArg(),
                    passwd=mox.IgnoreArg(),
                    use_unicode=True,
                    user=mox.IgnoreArg()).AndRaise(MySQLdb.Error)
    self.mox.ReplayAll()

    self.sql._connection = None
    self.assertRaises(logger.TransientError, self.sql.Open)

  def testClose(self):
    self.sql._connection = self.mox.CreateMock(MySQLdb.connections.Connection)
    self.sql._connection.close()
    self.mox.ReplayAll()

    self.sql.Close()

  def testQueryCallsOpen(self):
    self.mox.UnsetStubs()
    self.connection.cursor(mox.IgnoreArg()).AndReturn(self.mock_cursor)
    self.mock_cursor.execute(mox.IgnoreArg(), mox.IgnoreArg())
    self.mox.ReplayAll()

    self.sql._SQL__Query(True, True, (), False)

  def testQueryFetchesResults(self):
    self.mox.UnsetStubs()
    self.connection.cursor(mox.IgnoreArg()).AndReturn(self.mock_cursor)
    self.mock_cursor.execute(mox.IgnoreArg(), mox.IgnoreArg()).AndReturn('foo')
    self.mock_cursor.fetchall().AndReturn('bar')
    self.mox.ReplayAll()

    self.assertEquals(self.sql._SQL__Query(True, True, (), True),
                      ('foo', 'bar'))

  def testQueryErrors(self):
    self.mox.UnsetStubs()

    self.connection.cursor(mox.IgnoreArg()).AndReturn(self.mock_cursor)
    self.mock_cursor.execute(mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(
      MySQLdb.DataError)
    self.mox.ReplayAll()
    self.assertRaises(database.SQLPermanentError,
                      self.sql._SQL__Query, True, True, (), False)
    self.mox.ResetAll()

    self.connection.cursor(mox.IgnoreArg()).AndReturn(self.mock_cursor)
    self.mock_cursor.execute(mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(
      MySQLdb.IntegrityError)
    self.mox.ReplayAll()
    self.assertRaises(database.SQLPermanentError,
                      self.sql._SQL__Query, True, True, (), False)
    self.mox.ResetAll()

    self.connection.cursor(mox.IgnoreArg()).AndReturn(self.mock_cursor)
    self.mock_cursor.execute(mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(
      MySQLdb.ProgrammingError)
    self.mox.ReplayAll()
    self.assertRaises(database.SQLPermanentError,
                      self.sql._SQL__Query, True, True, (), False)
    self.mox.ResetAll()

    self.connection.cursor(mox.IgnoreArg()).AndReturn(self.mock_cursor)
    self.mock_cursor.execute(mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(
      MySQLdb.Error)
    self.mox.ReplayAll()
    self.assertRaises(database.SQLTransientError,
                      self.sql._SQL__Query, True, True, (), False)
    self.mox.ResetAll()

    self.connection.cursor(mox.IgnoreArg()).AndReturn(self.mock_cursor)
    self.mock_cursor.execute(mox.IgnoreArg(), mox.IgnoreArg()).AndRaise(
      MySQLdb.Warning)
    self.mox.ReplayAll()
    self.assertEquals((False, None),
                      self.sql._SQL__Query(True, True, (), False))
    self.mox.ResetAll()

  def testUpdate(self):
    self.mox.StubOutWithMock(self.sql, 'Execute')
    self.sql.Execute('UPDATE foo SET bar = %s WHERE coin = %s',
                     [42, 'coin'])
    self.mox.ReplayAll()

    self.sql.Update('foo', {'bar': 42}, {'coin': 'coin'})

  def testInsert(self):
    self.mox.StubOutWithMock(self.sql, 'Execute')
    self.sql.Execute('INSERT INTO foo SET bar = %s', ['pan'])
    self.mox.ReplayAll()

    self.sql.Insert('foo', {'bar': 'pan'})

  def testExecute(self):
    self.sql._SQL__Query(mox.IgnoreArg(),
                         'query', ('args',)).AndReturn((1, 2))
    self.mox.ReplayAll()

    self.assertEquals(self.sql.Execute('query', ('args', )), 1)

  def testQuery(self):
    self.sql._SQL__Query(mox.IgnoreArg(),
                         'query', ('args',), fetch=True).AndReturn((1, 2))
    self.mox.ReplayAll()

    self.assertEquals(self.sql.Query('query', ('args', )), 2)
