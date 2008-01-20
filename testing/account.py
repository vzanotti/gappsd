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
import testing.database
import unittest

class TestAccount(unittest.TestCase):
  _ACCOUNT_DICT = {
    "g_account_name": "foo.bar",
    "g_first_name": "foo",
    "g_admin": True,
  }

  def setUp(self):
    self.sql = testing.database.MockSQL()
    self.account = account.Account("foo.bar", self._ACCOUNT_DICT)

  def testLoadFromDatabase(self):
    self.sql.query_result = None
    self.assertEquals(
      account.LoadAccountFromDatabase(self.sql, "bar.foo"),
      None)

    self.sql.query_result = [self._ACCOUNT_DICT]
    a = account.LoadAccountFromDatabase(self.sql, "foo.bar")
    self.assertEquals(self.sql.query_args, ('foo.bar',))
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
    self.assertRaises(account.AccountActionError,
                      self.account.Create, self.sql)

  def testCreate(self):
    self.account.set("g_last_name", "bar")
    self.sql.insert_result = None
    self.account.Create(self.sql)

    self.assertEquals(self.sql.insert_table, "gapps_accounts")
    self.assertEquals(
      self.sql.insert_values,
      {'g_first_name': 'foo', 'g_last_name': 'bar', 'g_admin': True,
       'g_account_name': 'foo.bar'})

  def testUpdate(self):
    self.account.set("g_status", account.Account.STATUS_DISABLED)
    self.sql.update_result = None
    self.account.Update(self.sql)

    self.assertEquals(self.sql.update_table, "gapps_accounts")
    self.assertEquals(self.sql.update_values, {'g_status': 'disabled'})
    self.assertEquals(self.sql.update_where, {'g_account_name': 'foo.bar'})

  def testDelete(self):
    self.sql.execute_query = None
    self.account.Delete(self.sql)

    self.assertEquals(self.sql.execute_query,
                      "DELETE FROM gapps_accounts WHERE g_account_name = %s")
    self.assertEquals(self.sql.execute_args, ("foo.bar",))
