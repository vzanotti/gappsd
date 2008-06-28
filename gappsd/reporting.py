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

"""Implements the two jobs (ActivityJob and AccountsJob) working with the
Reporting API of Google Apps."""

import csv
import datetime
import google.reporting
import pytz

import account, job, queue
from . import logger
from .logger import PermanentError, TransientError

class ActivityJob(job.Job):
  """Implements the 'r_activity' job, which aims at updating the database
  version of the statistics/metrics offered by the Summary and Activity reports
  of the Reporting API."""

  _SQL_FIELDS = [
    "date", "num_accounts", "count_1_day_actives", "count_7_day_actives",
    "count_14_day_actives", "count_30_day_actives", "count_30_day_idle",
    "count_60_day_idle", "count_90_day_idle", "usage_in_bytes", "quota_in_mb",
  ]

  def __init__(self, config, sql, job_dict):
    job.Job.__init__(self, config, sql, job_dict)

  # Database interaction.
  def _GetLastReportDate(self):
    """Returns the date of the last report in the database, or the first day
    of the backlog if none is available."""

    last_known_report = self._sql.Query(
      "SELECT UNIX_TIMESTAMP(MAX(date)) AS date FROM gapps_reporting")
    if last_known_report[0]["date"] is None:
      report_backlog = self._config.get_int("gappsd.activity-backlog")
      return datetime.date.today() - datetime.timedelta(report_backlog)
    return datetime.date.fromtimestamp(last_known_report[0]["date"])

  def _ListDaysToProcess(self, report_limit=None):
    """Lists the days without activity reports, with a limited backlog.
    If @p last_report is None, it will get it from the database.
    """

    latest_report = \
      GetReportingApiClientInstance(self._config).GetLatestReportDate()
    if report_limit is None:
      report = self._GetLastReportDate() + datetime.timedelta(1)
    else:
      report = report_limit + datetime.timedelta(1)

    report_list = []
    while report <= latest_report:
      report_list.append(report)
      report += datetime.timedelta(1)
    return report_list

  def _StoreReport(self, date, report):
    """Stores the @p activity values in the database, for the @p date."""

    if "date" not in report:
      report["date"] = date
    self._sql.Insert("gapps_reporting",
      dict([k_v for k_v in list(report.items()) if k_v[0] in self._SQL_FIELDS]))

  # Job processing.
  def RunDailyReport(self, date, first_date):
    """Fetches the activity and summary reports, merges them, and add them to
    the database."""

    reporting_client = GetReportingApiClientInstance(self._config)
    activity_reports = reporting_client.GetReport(date, "activity") or []
    summary_reports = reporting_client.GetReport(date, "summary") or []
    first_date = int(first_date.strftime("%Y%m%d"))

    reports = {}
    for report in activity_reports:
      if "date" in report:
        reports[report["date"]] = report
    for report in summary_reports:
      if "date" in report:
        reports.setdefault(report["date"], {}).update(report)

    daily_reports = 0
    for (report_date, report) in list(reports.items()):
      if int(report_date) >= first_date:
        self._StoreReport(report_date, report)
        daily_reports += 1
    return daily_reports

  def Run(self):
    """Requests the required daily reports, and run them. Due to the specifities
    of the Activity and Summary reports, the daily report is run (for a month)
    on the last day in date_list."""

    last_report = None
    daily_reports = 0

    while True:
      date_list = self._ListDaysToProcess(last_report)
      if not len(date_list):
        break

      date = date_list[0]
      date = max([d for d in date_list \
                    if d.year == date.year and d.month == date.month])
      daily_reports += self.RunDailyReport(date, date_list[0])
      last_report = date

    self.Update(self.STATUS_SUCCESS, "%d days processed" % daily_reports)


class AccountsJob(job.Job):
  """Implementation of the 'r_accounts' job, which updates the database-stored
  version of the Google Apps accounts, using the Accounts report of the
  Reporting API."""

  # Format: <reporting field>: [<account field>, <silent synchronization>]
  _FIELDS = {
    "account_id":         ["g_account_id",   True],
    "usage_in_bytes":     ["r_disk_usage",   True],
    "creation_date":      ["r_creation",     True],
    "last_login_date":    ["r_last_login",   True],
    "last_web_mail_date": ["r_last_webmail", True],
    "surname":            ["g_last_name",    False],
    "given_name":         ["g_first_name",   False],
    "suspension_reason":  ["g_suspension",   True],
  }

  def __init__(self, config, sql, job_dict):
    job.Job.__init__(self, config, sql, job_dict)

  # SQL Account vs. GApps Account synchronization methods.
  def SynchronizeSQLAccount(self, sql):
    """Synchronizes the SQL account based on the fact the account did not
    show up in the reporting log."""

    if sql["g_status"] != "unprovisioned":
      queue.CreateQueueJob(self._sql, 'u_sync',
                           {"username": sql["g_account_name"]})

  def SynchronizeReportingAccount(self, reporting):
    """Creates a SQL account based on the Reporting version of the account."""

    queue.CreateQueueJob(self._sql, 'u_sync',
                         {"username": reporting["account_name"]})

  def SynchronizeSQLReportingAccounts(self, sql, reporting):
    """Synchronizes the SQL version of the account with the reporting version.
    If the data mismatches, a UserSync job is started (as the reporting version
    lags at least 12h behind the up-to-date version)."""

    a = account.Account(sql["g_account_name"], sql)
    create_sync_job = False
    for (key, (account_key, silent_update)) in list(self._FIELDS.items()):
      if key in reporting and a.get(account_key) != reporting[key]:
        if silent_update:
          a.set(account_key, reporting[key])
        else:
          create_sync_job = True

    a.Update(self._sql)
    if create_sync_job:
      queue.CreateQueueJob(self._sql, 'u_sync',
                           {"username": a.get("g_account_name")})


  # Account list retrieval.
  def FetchSQLAccounts(self):
    """Retrieves the list of all Google Apps accounts registered in the
    database. Returns a username-indexed dictionary of dictionaries."""

    sql_select = ", ".join([
      "g_account_id", "g_account_name", "g_first_name", "g_last_name",
      "g_status", "g_suspension", "r_disk_usage",
      "DATE_FORMAT(r_creation, '%%Y%%m%%d') AS r_creation",
      "DATE_FORMAT(r_last_login, '%%Y%%m%%d') AS r_last_login",
      "DATE_FORMAT(r_last_webmail, '%%Y%%m%%d') AS r_last_webmail",
    ])
    accounts = self._sql.Query("SELECT %s FROM gapps_accounts" % sql_select)
    return dict([(account["g_account_name"], account) for account in accounts])

  def FetchReportingAccounts(self):
    """Retrieves the list of all Google Apps accounts using the Reporting API.
    Returns a list of dictionaries."""

    reporting_client = GetReportingApiClientInstance(self._config)
    date = reporting_client.GetLatestReportDate()
    return reporting_client.GetReport(date, "accounts") or []

  def Run(self):
    """Retrieves accounts from the two sources to synchronize (SQL and
    Reporting), and synchronizes each account individually."""

    sql_accounts = self.FetchSQLAccounts()
    reporting_accounts = self.FetchReportingAccounts()
    for r_account in reporting_accounts:
      try:
        r_account["account_name"] = r_account["account_name"].split("@")[0]
        r_account["surname"] = r_account["surname"].decode("utf8")
        r_account["given_name"] = r_account["given_name"].decode("utf8")
        s_account = sql_accounts[r_account["account_name"]]
        self.SynchronizeSQLReportingAccounts(s_account, r_account)
        del sql_accounts[r_account["account_name"]]
      except KeyError:
        self.SynchronizeReportingAccount(r_account)

    for s_account in list(sql_accounts.values()):
      self.SynchronizeSQLAccount(s_account)

    self.Update(self.STATUS_SUCCESS)

class ReportingApiClient(google.reporting.ReportRunner):
  """Implements the compatibility layer between the gappsd framework and the
  google-provided reporting API client.

  Example usage:
    reporting = ReportingApiClient(config)
    reporting.GetReport(datetime.date(2008, 1, 1), "activity")
    # reporting is an iterable csv object
  """

  def __init__(self, config):
    google.reporting.ReportRunner.__init__(self)
    self.domain = config.get_string("gapps.domain")
    self.admin_email = \
      "%s@%s" % (config.get_string("gapps.admin-api-username"), self.domain)
    self.admin_password = config.get_string("gapps.admin-api-password")
    self.token_expiration = None
    self.token_validity = config.get_int("gappsd.token-expiration")

  def _RenewToken(self):
    """Checks that the token isn't expired yet, and renew it if it is."""

    if self.token is None or self.token_expiration < datetime.datetime.now():
      try:
        logger.info("Reporting API - Requesting authentication token")
        self.Login()
        self.token_expiration = \
          datetime.datetime.now() + datetime.timedelta(0, self.token_validity)
        logger.info("Reporting API - Authentication succedeed")
      except google.reporting.ConnectionError, error:
        logger.info("Reporting API - Authentication failed with unknown error")
        raise TransientError( \
          "ConnectionError in Reporting API authentication: %s" % error)
      except google.reporting.LoginError, error:
        if str(error) == 'Authentication failure':
          logger.critical("Reporting API - Authentication refused")
          raise logger.CredentialError("Bad credential for Reporting API")
        raise TransientError("LoginError: %s" % error)

  def LogOut(self):
    """Invalidates the current token, by calling the logout method on
    Google-side. Should be called whenever the token will not be used in the
    future."""

    # TODO(vzanotti): implement.
    pass

  @staticmethod
  def GetLatestReportDate(now_pst=None):
    """Returns the date of latest available report. Rule: after 12PM PST,
    last report is one day ago, otherwise last report is two days ago.
    Use @p now as current date/time if not None.
    """
    if not now_pst:
      now_pst = datetime.datetime.now(pytz.timezone("America/Los_Angeles"))
    return now_pst.date() - datetime.timedelta(2 if now_pst.hour < 12 else 1)

  def GetReport(self, date, report_name):
    """Fetches the report using the reporting API client, handles the client
    errors, and parses the csv result."""
    self._RenewToken()

    request = google.reporting.ReportRequest()
    request.token = self.token
    request.domain = self.domain
    request.report_name = report_name
    request.date = date.strftime("%Y-%m-%d")

    try:
      logger.info("Reporting API - Obtaining report '%s' for '%s'" % \
                  (report_name, request.date))
      report = self.GetReportData(request)
    except google.reporting.ConnectionError, error:
      raise TransientError("ConnectionError: %s" % error)
    except google.reporting.ReportError, error:
      # Permanent error code requiring admin intervention.
      if error.reason_code in [1001, 1004, 1005, 1007, 1027]:
        logger.error("Reporting API - Request failed\n%s" % error)
        raise PermanentError("In reporting: %s" % error)

      # Temporary error code meaning "report not found".
      if error.reason_code in [1045, 1059, 1060]:
        return None

      # Temporary error code meaning "retry".
      if error.reason_code in [1000, 1011, 1070]:
        logger.info("Reporting API - Transient report failure\n%s" % error)
        raise TransientError("Temporary reporting failure: %s" % error)

      # Authentication failure.
      if error.reason_code == 1006:
        self.token = None
        raise TransientError("Authentication token expired.")

      # Unknow error, mail the administrators.
      logger.error("Reporting API - Unknown error\n%s" % error)
      raise TransientError("Unkown error in reporting: %s" % error)

    return csv.DictReader(report.split("\n"))

def GetReportingApiClientInstance(config=None):
  """Returns the global reporting API client instance, and instantiates it
  if needed. Returns None if there is no current client and config is None."""
  global reporting_api_client
  if reporting_api_client is None:
    if not config:
      return None
    reporting_api_client = ReportingApiClient(config)
  return reporting_api_client

# Module initialization.
reporting_api_client = None
job.job_registry.Register('r_activity', ActivityJob)
job.job_registry.Register('r_accounts', AccountsJob)
