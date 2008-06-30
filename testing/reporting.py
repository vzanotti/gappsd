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
import gappsd.database as database
import gappsd.job as job
import gappsd.logger as logger
import gappsd.reporting as reporting
import google.reporting
import testing.config
import time
import mox, unittest

class TestActivityJob(mox.MoxTestBase):
  _ACTIVITY_JOB_DATA = {
    "q_id": 42, "p_status": "active", "p_entry_date": 1200043549,
    "p_start_date": 1200043559, "j_type": "r_activity", "j_parameters": "{}",
    "r_softfail_count": 0, "r_softfail_date": 1200043259,
  }

  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.client = self.mox.CreateMock(reporting.ReportingApiClient)
    self.config = testing.config.MockConfig()
    self.sql = self.mox.CreateMock(database.SQL)
    reporting.reporting_api_client = self.client

    self.activity = \
      reporting.ActivityJob(self.config, self.sql, self._ACTIVITY_JOB_DATA)

  def testGetLastReportDate(self):
    self.sql.Query(mox.IgnoreArg()).AndReturn([{"date": None}])
    self.sql.Query(mox.IgnoreArg()).AndReturn([{"date": 1167608563}])
    self.mox.ReplayAll()

    self.assertEquals(self.activity._GetLastReportDate(),
                      datetime.date.today() - datetime.timedelta(30))
    self.assertEquals(self.activity._GetLastReportDate(),
                      datetime.date(2007, 1, 1))

  def testListDaysToProcess(self):
    self.client.GetLatestReportDate().AndReturn(datetime.date(2007, 1, 1))
    self.sql.Query(mox.IgnoreArg()).AndReturn([{"date": 1167435763}])
    self.client.GetLatestReportDate().AndReturn(datetime.date(2007, 1, 1))
    self.mox.ReplayAll()

    yesterday = datetime.date(2007, 1, 1)
    two_days_ago = yesterday - datetime.timedelta(1)
    two_days = self.activity._ListDaysToProcess()
    one_day = self.activity._ListDaysToProcess(two_days_ago)

    self.assertEquals(two_days, [two_days_ago, yesterday])
    self.assertEquals(one_day, [yesterday])

  def testRunDailyReport(self):
    date20070101 = datetime.date(2007, 1, 1)
    date20070102 = datetime.date(2007, 1, 2)

    self.client.GetReport(datetime.date(2007, 1, 1), 'activity').AndReturn(
      [{"date": "20070101", "usage_in_bytes": 42}])
    self.client.GetReport(datetime.date(2007, 1, 1), 'summary').AndReturn(
      [{"date": "20070101", "quota_in_mb": 69}])
    self.sql.Insert('gapps_reporting',
                    {"date": "20070101", "usage_in_bytes": 42, "quota_in_mb": 69})
    self.mox.ReplayAll()
    self.assertEquals(self.activity.RunDailyReport(date20070101,
                                                   date20070101), 1)
    self.mox.ResetAll()

    reporting.reporting_api_client.reports = [
      (date20070101, "summary", [{"date": "20070101", "quota_in_mb": 69}]),
      (date20070101, "activity", [{"date": "20070101", "usage_in_bytes": 42}]),
    ]


    self.client.GetReport(datetime.date(2007, 1, 1), 'activity').AndReturn(
      [{"date": "20070101", "usage_in_bytes": 42}])
    self.client.GetReport(datetime.date(2007, 1, 1), 'summary').AndReturn(
      [{"date": "20070101", "quota_in_mb": 69}])
    self.mox.ReplayAll()
    self.assertEquals(self.activity.RunDailyReport(date20070101,
                                                   date20070102), 0)
    self.mox.ResetAll()

  def testRun(self):
    yesterday = datetime.date(2007, 1, 1)
    two_days_ago = yesterday - datetime.timedelta(1)

    self.client.GetLatestReportDate().AndReturn(datetime.date(2007, 1, 1))
    self.sql.Query(mox.IgnoreArg()).AndReturn([{"date": 1167522163}])
    self.client.GetReport(yesterday, 'activity')
    self.client.GetReport(yesterday, 'summary').AndReturn(
      [{"date": "20070102", "usage_in_bytes": 43}])
    self.sql.Insert(mox.IgnoreArg(),
                    mox.And(mox.ContainsKeyValue('date', '20070102'),
                            mox.ContainsKeyValue('usage_in_bytes', 43)))
    self.client.GetLatestReportDate().AndReturn(datetime.date(2007, 1, 1))
    self.mox.StubOutWithMock(self.activity, 'Update')
    self.activity.Update(job.Job.STATUS_SUCCESS, mox.IgnoreArg())
    self.mox.ReplayAll()

    self.activity.Run()


class TestAccountsReport(mox.MoxTestBase):
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

  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.client = self.mox.CreateMock(reporting.ReportingApiClient)
    self.config = testing.config.MockConfig()
    self.sql = self.mox.CreateMock(database.SQL)
    reporting.reporting_api_client = self.client

    self.accounts = \
      reporting.AccountsJob(self.config, self.sql, self._ACCOUNTS_JOB_DATA)

  def testSynchronizeSQLAccount(self):
    self.sql.Insert('gapps_queue',
                    mox.ContainsKeyValue('j_parameters',
                                         '{"username": "foo.bar"}'))
    self.mox.ReplayAll()
    self.accounts.SynchronizeSQLAccount({
      "g_account_name": "foo.bar",
      "g_status": "active",
    })
    self.mox.ResetAll()

    self.mox.ReplayAll()
    self.accounts.SynchronizeSQLAccount({
      "g_account_name": "qux.quz",
      "g_status": "unprovisioned",
    })

  def testSynchronizeReportingAccount(self):
    self.sql.Insert('gapps_queue',
                    mox.ContainsKeyValue('j_parameters',
                                         '{"username": "foo.bar"}'))
    self.mox.ReplayAll()

    self.accounts.SynchronizeReportingAccount({
      "account_name": "foo.bar",
      "creation_date": "20070101",
    })

  def testSynchronizeSQLReportingAccounts(self):
    # Test the synchronization of reporting-owned values.
    self.sql.Update('gapps_accounts',
                    mox.ContainsKeyValue('r_disk_usage', 69),
                    mox.IgnoreArg())
    self.sql.Insert('gapps_accounts', None)
    self.mox.ReplayAll()
    self.accounts.SynchronizeSQLReportingAccounts(self._ACCOUNT_DICT,
      {"usage_in_bytes": 69, "last_login_date": "20070101"})
    self.mox.ResetAll()

    # Test the synchronization of values owned by the provisioning API.
    self.sql.Update('gapps_accounts',
                    mox.ContainsKeyValue('r_last_webmail', '20070102'),
                    mox.IgnoreArg())
    self.sql.Insert('gapps_queue',
                    mox.ContainsKeyValue('j_parameters',
                                         '{"username": "foo.bar"}'))
    self.mox.ReplayAll()
    self.accounts.SynchronizeSQLReportingAccounts(self._ACCOUNT_DICT,
      {"surname": "qux", "last_web_mail_date": "20070102"})
    self.mox.ResetAll()

  def testFetchSQLAccounts(self):
    self.sql.Query(mox.IgnoreArg()).AndReturn([{"g_account_name": "foo.bar"}])
    self.mox.ReplayAll()

    a = self.accounts.FetchSQLAccounts()
    self.assertEquals(a, {"foo.bar": {"g_account_name": "foo.bar"}})

  def testFetchReportingAccounts(self):
    self.client.GetLatestReportDate().AndReturn(datetime.date(2007, 1, 1))
    self.client.GetReport(datetime.date(2007, 1, 1), 'accounts').AndReturn([])
    self.mox.ReplayAll()

    self.accounts.FetchReportingAccounts()

  def testRun(self):
    self.mox.StubOutWithMock(self.accounts, 'SynchronizeSQLAccount')
    self.mox.StubOutWithMock(self.accounts, 'SynchronizeReportingAccount')
    self.mox.StubOutWithMock(self.accounts, 'SynchronizeSQLReportingAccounts')
    self.mox.StubOutWithMock(self.accounts, 'Update')

    # Account which requires a SQL <-> Reporting synchronization.
    self.sql.Query(mox.IgnoreArg()).AndReturn([{"g_account_name": "foo.bar"}])
    self.client.GetLatestReportDate().AndReturn(datetime.date(2007, 1, 1))
    self.client.GetReport(datetime.date(2007, 1, 1), 'accounts').AndReturn([{
      "account_name": "foo.bar@a.b",
      "surname": "foo",
      "given_name": "bar",
    }])
    self.accounts.SynchronizeSQLReportingAccounts(mox.IgnoreArg(), mox.IgnoreArg())
    self.accounts.Update(job.Job.STATUS_SUCCESS)
    self.mox.ReplayAll()
    self.accounts.Run()
    self.mox.ResetAll()

    # Account which requires a SQL <- Reporting synchronization.
    self.sql.Query(mox.IgnoreArg()).AndReturn([{"g_account_name": "qux.quz"}])
    self.client.GetLatestReportDate().AndReturn(datetime.date(2007, 1, 1))
    self.client.GetReport(datetime.date(2007, 1, 1), 'accounts').AndReturn([])
    self.accounts.SynchronizeSQLAccount(mox.IgnoreArg())
    self.accounts.Update(job.Job.STATUS_SUCCESS)
    self.mox.ReplayAll()
    self.accounts.Run()
    self.mox.ResetAll()

    # Account which requires a SQL -> Reporting synchronization.
    self.sql.Query(mox.IgnoreArg()).AndReturn([])
    self.client.GetLatestReportDate().AndReturn(datetime.date(2007, 1, 1))
    self.client.GetReport(datetime.date(2007, 1, 1), 'accounts').AndReturn([{
      "account_name": "foo.bar@a.b",
      "surname": "foo",
      "given_name": "bar",
    }])
    self.accounts.SynchronizeReportingAccount(mox.IgnoreArg())
    self.accounts.Update(job.Job.STATUS_SUCCESS)
    self.mox.ReplayAll()
    self.accounts.Run()
    self.mox.ResetAll()


class TestReportingApiClient(mox.MoxTestBase):
  # Redefinition of google.reporting.ReportRunner methods.
  def Login(self):
    if self.reporting.admin_email is None or \
       self.reporting.admin_password is None:
      raise google.reporting.LoginError( \
        "TestReporting: email or password undefined.")
    if self.login_result is None:
      return None
    raise self.login_result

  def GetReportData(self, request):
    self.request = request
    if isinstance(self.request_result, str):
      return self.request_result
    raise self.request_result

  # Tests.
  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.reporting = reporting.ReportingApiClient(testing.config.MockConfig())
    self.mox.StubOutWithMock(self.reporting, 'Login')
    self.mox.StubOutWithMock(self.reporting, 'GetReportData')

  def testRenewTokenNotExpired(self):
    self.reporting.token = "token"
    self.reporting.token_expiration = \
      datetime.datetime.now() + datetime.timedelta(1)
    self.mox.ReplayAll()

    self.reporting._RenewToken()

  def testRenewTokenExpiredNormal(self):
    self.reporting.token = "token"
    self.reporting.token_expiration = \
      datetime.datetime.now() - datetime.timedelta(0, 1)
    self.reporting.Login()
    self.mox.ReplayAll()

    self.reporting._RenewToken()

  def testRenewTokenError(self):
    self.reporting.Login().AndRaise(
      google.reporting.ConnectionError('', ''))
    self.reporting.Login().AndRaise(
      google.reporting.LoginError('Authentication failure'))
    self.reporting.Login().AndRaise(google.reporting.LoginError('Foo'))
    self.mox.ReplayAll()

    self.assertRaises(logger.TransientError, self.reporting._RenewToken)
    self.assertRaises(logger.CredentialError, self.reporting._RenewToken)
    self.assertRaises(logger.TransientError, self.reporting._RenewToken)

  def testGetLatestReportDate(self):
    date20061231 = datetime.date(2006, 12, 31)
    date20070101 = datetime.date(2007, 1, 1)
    self.assertEquals(reporting.ReportingApiClient.GetLatestReportDate(
      datetime.datetime(2007, 1, 2, 11, 59, 59)), date20061231)
    self.assertEquals(reporting.ReportingApiClient.GetLatestReportDate(
      datetime.datetime(2007, 1, 2, 12, 0, 1)), date20070101)

  def testGetReport(self):
    def ValidReportRequest(report):
      return report.token == 'token' and report.domain == 'GD' and \
        report.date == '2006-12-31' and report.report_name == 'activity'

    self.reporting.token = "token"
    self.reporting.token_expiration = \
      datetime.datetime.now() - datetime.timedelta(0, 1)
    self.reporting.Login()
    self.reporting.GetReportData(mox.Func(ValidReportRequest)).AndReturn('blih\n1\n2')
    self.mox.ReplayAll()

    r = self.reporting.GetReport(datetime.date(2006, 12, 31), 'activity')
    self.assertEquals([l for l in r], [{"blih": "1"}, {"blih": "2"}])

  def testGetReportErrors(self):
    # Test error handling for permanent API errors.
    kReportError1001 = google.reporting.ReportError()
    kReportError1001.reason_code = 1001
    self.reporting.Login()
    self.reporting.GetReportData(mox.IgnoreArg()).AndRaise(kReportError1001)
    self.mox.ReplayAll()
    self.assertRaises(logger.PermanentError, self.reporting.GetReport,
                      datetime.date(2006, 12, 31), 'activity')
    self.mox.ResetAll()

    # Test error handling for 'invalid token' API errors.
    kReportError1006 = google.reporting.ReportError()
    kReportError1006.reason_code = 1006
    self.reporting.Login()
    self.reporting.GetReportData(mox.IgnoreArg()).AndRaise(kReportError1006)
    self.mox.ReplayAll()
    self.assertRaises(logger.TransientError, self.reporting.GetReport,
                      datetime.date(2006, 12, 31), 'activity')
    self.assertEquals(self.reporting.token, None)
    self.mox.ResetAll()

    # Test error handling for transient API errors.
    self.reporting.Login()
    self.reporting.GetReportData(mox.IgnoreArg()).AndRaise(
      google.reporting.ConnectionError("", ""))
    self.mox.ReplayAll()
    self.assertRaises(logger.TransientError, self.reporting.GetReport,
                      datetime.date(2006, 12, 31), 'activity')
    self.mox.ResetAll()

  def testGetAbsentReport(self):
    kReportError1045 = google.reporting.ReportError()
    kReportError1045.reason_code = 1045;
    self.reporting.Login()
    self.reporting.GetReportData(mox.IgnoreArg()).AndRaise(kReportError1045)
    self.mox.ReplayAll()

    self.reporting.GetReport(datetime.date(2006, 12, 31), 'activity')
