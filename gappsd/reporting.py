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
import pytz

import account, api, job, queue
from . import logger
from .logger import PermanentError, TransientError

class ActivityJob(job.Job):
  """Implements the 'r_activity' job, which aims at updating the database
  version of the statistics/metrics offered by the Summary and Activity reports
  of the Reporting API."""
  
  _REPORT_PARAMETERS = {  # <API parameter name>: <sql field name>
    "accounts:num_1day_logins": "count_1_day_actives",
    "accounts:num_7day_logins": "count_7_day_actives",
    "accounts:num_30day_logins": "count_30_day_actives",
    "accounts:used_quota_in_mb": "usage_in_bytes",
  }

  PROP__SIDE_EFFECTS = False

  def __init__(self, config, sql, job_dict):
    job.Job.__init__(self, config, sql, job_dict)
    self._api = api.GetReportsService(config)

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
    
    now = datetime.datetime.now(pytz.timezone("America/Los_Angeles"))
    latest_report = now.date() - datetime.timedelta(2 if now.hour < 12 else 1)
    if report_limit is None:
      report = self._GetLastReportDate() + datetime.timedelta(1)
    else:
      report = report_limit + datetime.timedelta(1)

    report_list = []
    while report <= latest_report:
      report_list.append(report)
      report += datetime.timedelta(1)
    return report_list

  # Job processing.
  def RunDailyReport(self, date, first_date):
    """Fetches the activity and summary reports, merges them, and add them to
    the database."""
    
    # Prepare and execute the report.
    api_request = self._api.customerUsageReports().get(
        date=date.strftime("%Y-%m-%d"),
        parameters=','.join(self._REPORT_PARAMETERS.keys()))
    try:
      api_response = api_request.execute()
    except Exception as error:
      api.HandleError(error)
    
    # Extract the relevant information.
    results = dict()
    for entry in api_response['usageReports'][0]['parameters']:
      results_key = self._REPORT_PARAMETERS[entry['name']]
      results[results_key] = entry['intValue']
    
    # Fix the type of accounts:used_quota_in_mb, and store the report.
    results['date'] = api_response['usageReports'][0]['date']
    results['usage_in_bytes'] = int(results['usage_in_bytes']) * 1024 * 1024
    self._sql.Insert("gapps_reporting", results)
    return 1

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
    "creation_date":      ["r_creation",     True],
    "surname":            ["g_last_name",    False],
    "given_name":         ["g_first_name",   False],
    "suspension_reason":  ["g_suspension",   True],
  }
  

  PROP__SIDE_EFFECTS = False

  def __init__(self, config, sql, job_dict):
    job.Job.__init__(self, config, sql, job_dict)
    self._api = api.GetDirectoryService(config)

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
    """Retrieves the list of all Google Apps accounts using the Directory API.
    Returns a list of dictionaries."""
    
    api_request = self._api.users().list(
        customer=self._config.get_string("gapps.customer"),
        maxResults=500)  # 500 is maximum allowable value
    while api_request:
      try:
        api_response = api_request.execute()
      except Exception as error:
        api.HandleError(error)
      for user in api_response['users']:
        yield {
          'account_name': user['primaryEmail'].split("@")[0],
          'creation_date': user['creationTime'][0:10],
          'given_name': user['name']['givenName'],
          'surname': user['name']['familyName'],
          'suspension_reason': user.get('suspensionReason', None),
        }
      
      api_request = self._api.users().list_next(api_request, api_response)

  def Run(self):
    """Retrieves accounts from the two sources to synchronize (SQL and
    Reporting), and synchronizes each account individually."""

    sql_accounts = self.FetchSQLAccounts()
    reporting_accounts = list(self.FetchReportingAccounts())
    for r_account in reporting_accounts:
      try:
        if "suspension_reason" in r_account and r_account["suspension_reason"]:
          r_account["suspension_reason"] = r_account["suspension_reason"][0:256]
        s_account = sql_accounts[r_account["account_name"]]
        self.SynchronizeSQLReportingAccounts(s_account, r_account)
        del sql_accounts[r_account["account_name"]]
      except KeyError:
        self.SynchronizeReportingAccount(r_account)
 
    for s_account in list(sql_accounts.values()):
      self.SynchronizeSQLAccount(s_account)
 
    self.Update(self.STATUS_SUCCESS)

# Module initialization.
job.job_registry.Register('r_activity', ActivityJob)
job.job_registry.Register('r_accounts', AccountsJob)
