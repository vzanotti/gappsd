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

import datetime
import gappsd.logger as logger
import gappsd.reporting as reporting
import google.reporting as greporting
import testing.config
import testing.database
import time
import unittest

class MockReportingApiClient(object):
  """Mock version of the ReportingApiClient, used to test Activity and Accounts
  jobs. Usage: self.reports contain a sequence of (date, name, result), which
  are returned to the MockReportingApiClient user. Last element of the sequence
  is served first.
  """

  def __init__(self):
    self.reports = []

  def GetLatestReportDate(self, now_pst=None):
    return datetime.date(2007, 1, 1)

  def GetReport(self, date, report_name):
    if not len(self.reports):
      raise logger.TransientError, \
        "Unexpected request (%s, %s)" % (date, report_name)
    if date != self.reports[-1][0]:
      self.reports.pop()
      raise logger.TransientError, "Bad report date"
    if report_name != self.reports[-1][1]:
      self.reports.pop()
      raise logger.TransientError, "Bad report name"

    return self.reports.pop()[2]


class TestActivityJob(unittest.TestCase):
  _ACTIVITY_JOB_DATA = {
    "q_id": 42, "p_status": "active", "p_entry_date": 1200043549,
    "p_start_date": 1200043559, "j_type": "r_activity", "j_parameters": "{}",
    "r_softfail_count": 0, "r_softfail_date": 1200043259,
  }

  def setUp(self):
    self.config = testing.config.MockConfig()
    self.sql = testing.database.MockSQL()
    self.activity = \
      reporting.ActivityJob(self.config, self.sql, self._ACTIVITY_JOB_DATA)
    reporting.reporting_api_client = MockReportingApiClient()

  def testGetLastReportDate(self):
    self.sql.query_result = [{"date": None}]
    self.assertEquals(self.activity._GetLastReportDate(),
                      datetime.date.today() - datetime.timedelta(30))

    self.sql.query_result = [{"date": 1167608563}]
    self.assertEquals(self.activity._GetLastReportDate(),
                      datetime.date(2007, 1, 1))

  def testListDaysToProcess(self):
    yesterday = datetime.date(2007, 1, 1)
    two_days_ago = yesterday - datetime.timedelta(1)
    self.sql.query_result = [{"date": 1167435763}]

    two_days = self.activity._ListDaysToProcess()
    one_day = self.activity._ListDaysToProcess(two_days_ago)

    self.assertEquals(two_days, [two_days_ago, yesterday])
    self.assertEquals(one_day, [yesterday])

  def testRunDailyReport(self):
    date20070101 = datetime.date(2007, 1, 1)
    date20070102 = datetime.date(2007, 1, 2)
    reporting.reporting_api_client.reports = [
      (date20070101, "summary", [{"date": "20070101", "quota_in_mb": 69}]),
      (date20070101, "activity", [{"date": "20070101", "usage_in_bytes": 42}]),
      (date20070101, "summary", [{"date": "20070101", "quota_in_mb": 69}]),
      (date20070101, "activity", [{"date": "20070101", "usage_in_bytes": 42}]),
    ]

    self.assertEquals(self.activity.RunDailyReport(date20070101,
                                                   date20070101), 1)
    self.assertEquals(self.sql.insert_values, {
      "date": "20070101", "usage_in_bytes": 42, "quota_in_mb": 69
    })

    self.sql.insert_values = None
    self.assertEquals(self.activity.RunDailyReport(date20070101,
                                                   date20070102), 0)
    self.assertEquals(self.sql.insert_values, None)

  def testRun(self):
    yesterday = datetime.date(2007, 1, 1)
    two_days_ago = yesterday - datetime.timedelta(1)
    reporting.reporting_api_client.reports = [
      (yesterday, "summary", [{"date": "20070102", "usage_in_bytes": 43}]),
      (yesterday, "activity", None),
    ]
    self.sql.query_result = [{"date": 1167522163}]

    self.activity.Run()
    self.assertEquals(self.sql.insert_values, {
      "date": "20070102", "usage_in_bytes": 43
    })


class TestAccountsReport(unittest.TestCase):
  _ACCOUNTS_JOB_DATA = {
    "q_id": 42, "p_status": "active", "p_entry_date": 1200043549,
    "p_start_date": 1200043559, "j_type": "r_activity", "j_parameters": "{}",
    "r_softfail_count": 0, "r_softfail_date": 1200043259,
  }
  _ACCOUNT_DICT = {
    "g_account_name": "foo.bar", "g_account_id": "0000000042424242",
    "g_first_name": "foo", "g_last_name": "bar", "r_disk_usage": 42,
    "r_creation": "20070101", "r_last_login": "20070101",
    "r_last_webmail": "20070101", "g_suspension": None,
  }

  # Methods to be injected in the AccountsJob object.
  class SQLUsed(Exception):
    pass
  class ReportingUsed(Exception):
    pass
  class SQLReportingUsed(Exception):
    pass

  def SynchronizeSQLAccount(self, sql):
    raise self.SQLUsed
  def SynchronizeReportingAccount(self, reporting):
    raise self.ReportingUsed
  def SynchronizeSQLReportingAccounts(self, sql, reporting):
    raise self.SQLReportingUsed

  # Tests.
  def setUp(self):
    self.config = testing.config.MockConfig()
    self.sql = testing.database.MockSQL()
    self.accounts = \
      reporting.AccountsJob(self.config, self.sql, self._ACCOUNTS_JOB_DATA)
    reporting.reporting_api_client = MockReportingApiClient()

  def testSynchronizeSQLAccount(self):
    self.sql.insert_result = None
    self.accounts.SynchronizeSQLAccount({
      "g_account_name": "foo.bar",
      "g_status": "active",
    })
    self.assertEquals(self.sql.insert_values["j_parameters"],
                      '{"username": "foo.bar"}')

    self.sql.insert_values = None
    self.accounts.SynchronizeSQLAccount({
      "g_account_name": "qux.quz",
      "g_status": "unprovisioned",
    })
    self.assertEquals(self.sql.insert_values, None)

  def testSynchronizeReportingAccount(self):
    self.accounts.SynchronizeReportingAccount({
      "account_name": "foo.bar",
      "creation_date": "20070101",
    })
    self.assertEquals(self.sql.insert_values["j_parameters"],
                      '{"username": "foo.bar"}')

  def testSynchronizeSQLReportingAccounts(self):
    self.sql.insert_values = None
    self.accounts.SynchronizeSQLReportingAccounts(self._ACCOUNT_DICT,
      {"usage_in_bytes": 69, "last_login_date": "20070101"})
    self.assertEquals(self.sql.insert_values, None)
    self.assertEquals(self.sql.update_values["r_disk_usage"], 69)

    self.accounts.SynchronizeSQLReportingAccounts(self._ACCOUNT_DICT,
      {"surname": "qux", "last_web_mail_date": "20070102"})
    self.assertEquals(self.sql.insert_values["j_parameters"],
                      '{"username": "foo.bar"}')
    self.assertEquals(self.sql.update_values["r_last_webmail"], "20070102")

  def testFetchSQLAccounts(self):
    self.sql.query_query = None
    self.sql.query_result = [{"g_account_name": "foo.bar"}]
    a = self.accounts.FetchSQLAccounts()
    self.assertEquals(a, {"foo.bar": {"g_account_name": "foo.bar"}})

  def testFetchReportingAccounts(self):
    reporting.reporting_api_client.reports = [
      (datetime.date(2007, 1, 1), "accounts", [])
    ]
    self.accounts.FetchReportingAccounts()

  def testRun(self):
    self.accounts.SynchronizeSQLAccount = self.SynchronizeSQLAccount
    self.accounts.SynchronizeReportingAccount = self.SynchronizeReportingAccount
    self.accounts.SynchronizeSQLReportingAccounts = \
      self.SynchronizeSQLReportingAccounts

    self.sql.query_result = [{"g_account_name": "foo.bar"}]
    reporting.reporting_api_client.reports = [
      (datetime.date(2007, 1, 1), "accounts", [{"account_name": "foo.bar@a.b"}])
    ]
    self.assertRaises(self.SQLReportingUsed, self.accounts.Run)

    self.sql.query_result = [{"g_account_name": "qux.quz"}]
    reporting.reporting_api_client.reports = [
      (datetime.date(2007, 1, 1), "accounts", [])
    ]
    self.assertRaises(self.SQLUsed, self.accounts.Run)

    self.sql.query_result = []
    reporting.reporting_api_client.reports = [
      (datetime.date(2007, 1, 1), "accounts", [{"account_name": "foo.bar@a.b"}])
    ]
    self.assertRaises(self.ReportingUsed, self.accounts.Run)


class TestReportingApiClient(unittest.TestCase):
  # Redefinition of google.reporting.ReportRunner methods.
  def Login(self):
    if self.reporting.admin_email is None or \
       self.reporting.admin_password is None:
      raise greporting.LoginError("TestReporting: email or password undefined.")
    if self.login_result is None:
      return None
    raise self.login_result

  def GetReportData(self, request):
    self.request = request
    if type(self.request_result) is str:
      return self.request_result
    raise self.request_result

  # Tests.
  def setUp(self):
    self.login_result = None
    self.request = None
    self.request_result = None

    self.reporting = reporting.ReportingApiClient(testing.config.MockConfig())
    self.reporting.Login = self.Login
    self.reporting.GetReportData = self.GetReportData

  def testRenewTokenNotExpired(self):
    self.login_result = greporting.ConnectionError("", "")
    self.reporting.token = "token"
    self.reporting.token_expiration = \
      datetime.datetime.now() + datetime.timedelta(1)
    self.reporting._RenewToken()

  def testRenewTokenExpiredNormal(self):
    self.login_result = None
    self.reporting.token = "token"
    self.reporting.token_expiration = \
      datetime.datetime.now() - datetime.timedelta(0, 1)
    self.reporting._RenewToken()

  def testRenewTokenError(self):
    self.login_result = greporting.ConnectionError("", "")
    self.assertRaises(logger.TransientError, self.reporting._RenewToken)

    self.login_result = greporting.LoginError('Authentication failure')
    self.assertRaises(logger.CredentialError, self.reporting._RenewToken)

    self.login_result = greporting.LoginError('Foo')
    self.assertRaises(logger.TransientError, self.reporting._RenewToken)

  def testGetLatestReportDate(self):
    date20061231 = datetime.date(2006, 12, 31)
    date20070101 = datetime.date(2007, 1, 1)
    self.assertEquals(reporting.ReportingApiClient.GetLatestReportDate(
      datetime.datetime(2007, 1, 2, 11, 59, 59)), date20061231)
    self.assertEquals(reporting.ReportingApiClient.GetLatestReportDate(
      datetime.datetime(2007, 1, 2, 12, 0, 1)), date20070101)

  def testGetReport(self):
    self.request_result = "blih\n1\n2"
    self.reporting.token = "token"
    self.reporting.token_expiration = \
      datetime.datetime.now() - datetime.timedelta(0, 1)
    r = self.reporting.GetReport(datetime.date(2006, 12, 31), 'activity')

    self.assertEquals(self.request.domain, "GD")
    self.assertEquals(self.request.token, "token")
    self.assertEquals(self.request.report_name, "activity")
    self.assertEquals(self.request.date, "2006-12-31")
    self.assertEquals([l for l in r], [{"blih": "1"}, {"blih": "2"}])

  def testGetReportErrors(self):
    self.request_result = greporting.ReportError()
    self.request_result.reason_code = 1001
    self.assertRaises(logger.PermanentError, self.reporting.GetReport,
                      datetime.date(2006, 12, 31), 'activity')

    self.request_result.reason_code = 1006
    self.assertRaises(logger.TransientError, self.reporting.GetReport,
                      datetime.date(2006, 12, 31), 'activity')
    self.assertEquals(self.reporting.token, None)

    self.request_result = greporting.ConnectionError("", "")
    self.assertRaises(logger.TransientError, self.reporting.GetReport,
                      datetime.date(2006, 12, 31), 'activity')

  def testGetAbsentReport(self):
    self.request_result = greporting.ReportError()
    self.request_result.reason_code = 1045
    self.reporting.GetReport(datetime.date(2006, 12, 31), 'activity')
