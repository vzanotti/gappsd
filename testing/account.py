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

import gappsd.account as account
import gappsd.database as database
import mox, unittest

class TestAccount(mox.MoxTestBase):
  _ACCOUNT_DICT = {
    "g_account_name": "foo.bar",
    "g_first_name": "foo",
    "g_admin": True,
  }

  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.sql = self.mox.CreateMock(database.SQL)
    self.account = account.Account("foo.bar", self._ACCOUNT_DICT)

  def testStandardInit(self):
    a = account.Account('foo.bar')
    self.assertEquals(a.get('g_account_name'), 'foo.bar')

  def testInvalidAccountNameInit(self):
    self.assertRaises(account.AccountContentError,
                      account.Account,
                      'foo.bar', {'g_account_name': 'qux.quz'})

  def testLoadFromDatabase(self):
    self.sql.Query(mox.IgnoreArg(), mox.IgnoreArg())
    self.sql.Query(mox.IgnoreArg(), ('foo.bar',)).AndReturn([self._ACCOUNT_DICT])
    self.mox.ReplayAll()

    self.assertEquals(
      account.LoadAccountFromDatabase(self.sql, "bar.foo"),
      None)

    a = account.LoadAccountFromDatabase(self.sql, "foo.bar")
    self.assertEquals(a.get("g_account_name"), "foo.bar")
    self.assertEquals(a.get("g_admin"), True)

  def testMutator(self):
    self.assertRaises(account.AccountActionError,
                      self.account.set, "invalid-field", None)
    self.assertRaises(account.AccountActionError,
                      self.account.set, "g_account_name", None)
    self.account.set("g_last_name", "qux")
    self.assertEquals(self.account.get("g_last_name"), "qux")

  def testCreateMissingFields(self):
    self.sql.Query(mox.IgnoreArg(), mox.IgnoreArg())
    self.mox.ReplayAll()

    self.assertRaises(account.AccountActionError,
                      self.account.Create, self.sql)

  def testCreate(self):
    self.sql.Query(mox.IgnoreArg(), mox.IgnoreArg())
    self.sql.Insert('gapps_accounts',
                    {'g_first_name': 'foo', 'g_last_name': 'bar',
                     'g_admin': True, 'g_account_name': 'foo.bar'})
    self.mox.ReplayAll()

    self.account.set("g_last_name", "bar")
    self.account.Create(self.sql)

  def testCreateAlreadyExists(self):
    self.sql.Query(mox.IgnoreArg(),
                   mox.IgnoreArg()).AndReturn([self._ACCOUNT_DICT])
    self.mox.ReplayAll()

    self.account.set("g_last_name", "bar")
    self.assertRaises(account.AccountActionError, self.account.Create, self.sql)

  def testUpdate(self):
    self.sql.Update('gapps_accounts',
                    {'g_status': 'disabled'},
                    {'g_account_name': 'foo.bar'})
    self.mox.ReplayAll()

    self.account.set("g_status", account.Account.STATUS_DISABLED)
    self.account.Update(self.sql)

  def testDelete(self):
    self.sql.Execute('DELETE FROM gapps_accounts WHERE g_account_name = %s',
                     ('foo.bar',))
    self.mox.ReplayAll()

    self.sql.execute_query = None
    self.account.Delete(self.sql)
