#!/usr/bin/python
#
# Initial work: Copyright (C) 2007 Google Inc.
# Modified by: Vincent Zanotti (vincent.zanotti@polytechnique.org)
#
# Licensed under the Apache License, Version 2.0 (the "License")
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""This module implements a client for the Google Apps Reporting API.

The Google Apps Reporting API allows domain administrators the ability to
query user behaviour and resource usage for a given time frame.

The Google Apps Reporting API reference document is at:

http://code.google.com/apis/apps/reporting/google_apps_reporting_api.html

  ReportRequest:   Encapsulates attribute of a Reporting API request.
  Error:           Base error class.
  ReportError:     Error while executing report.
  ConnectionError: Error during HTTPS connection to a URL.
  ReportRunner:    Contains the web service calls to run a report.
  main():          Run a report with command-line arguments.
"""

import getopt
import re
import sys
import time
import urllib
import urllib2


class ReportRequest:

  """This class encapsulates the attributes of a Reporting API request."""

  _REQUEST_TEMPLATE = ('<?xml version="1.0" encoding="UTF-8"?>\n'
      '<rest xmlns="google:accounts:rest:protocol"\n'
      '    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n'
      '  <type>%(type)s</type>\n'
      '  <token>%(token)s</token>\n'
      '  <domain>%(domain)s</domain>\n'
      '  <date>%(date)s</date>\n'
      '  <reportType>%(report_type)s</reportType>\n'
      '  <reportName>%(report_name)s</reportName>\n'
      '</rest>\n')

  def __init__(self):
    """Initializes the report request with default values."""

    self.type = 'Report'
    self.token = None
    self.domain = None
    self.date = None
    self.report_type = 'daily'
    self.report_name = None

  def ToXml(self):
    """Return the XML request for the Reporting API.

    Returns:
      Reporting API XML request string.
    """
    return ReportRequest._REQUEST_TEMPLATE % self.__dict__


class Error(Exception):

  """Base error class."""

  pass


class LoginError(Error):

  """Unable to log in to authentication service."""

  pass

class ReportError(Error):

  """Report execution error class."""

  _ERROR_TEMPLATE = ('Error executing report:\n'
                     '  status=%(status)s\n'
                     '  reason=%(reason)s\n'
                     '  extended_message=%(extended_message)s\n'
                     '  result=%(result)s\n'
                     '  type=%(type)s\n')
  _STATUS_PATTERN  = re.compile(r'status>(.*?)<', re.DOTALL)
  _STATUS_CODE_PATTERN  = re.compile(r'status>.*\(([0-9]+)\)<', re.DOTALL)
  _REASON_PATTERN  = re.compile('reason>(.*?)<', re.DOTALL)
  _REASON_CODE_PATTERN  = re.compile('reason>.*\(([0-9]+)\)<', re.DOTALL)
  _EXTENDED_MESSAGE_PATTERN  = re.compile('extendedMessage>(.*?)<', re.DOTALL)
  _RESULT_PATTERN  = re.compile('result>(.*?)<', re.DOTALL)
  _TYPE_PATTERN  = re.compile('type>(.*?)<', re.DOTALL)

  def __init__(self):
    """Construct a report execution error."""

    Error.__init__(self, 'Error executing report')
    self.status = None
    self.status_code = None
    self.reason = None
    self.reason_code = None
    self.extended_message = None
    self.result = None
    self.type = None

  def FromXml(self, xml):
    """Unmarshall an error from a Reporting API XML rstring.

    Args:
      xml: Reporting API XML response string.
    """
    match = ReportError._STATUS_PATTERN.search(xml)
    if match is not None: self.status = match.group(1)
    match = ReportError._STATUS_CODE_PATTERN.search(xml)
    if match is not None: self.status_code = int(match.group(1))
    match = ReportError._REASON_PATTERN.search(xml)
    if match is not None: self.reason = match.group(1)
    match = ReportError._REASON_CODE_PATTERN.search(xml)
    if match is not None: self.reason_code = int(match.group(1))
    match = ReportError._EXTENDED_MESSAGE_PATTERN.search(xml)
    if match is not None: self.extended_message = match.group(1)
    match = ReportError._RESULT_PATTERN.search(xml)
    if match is not None: self.result = match.group(1)
    match = ReportError._TYPE_PATTERN.search(xml)
    if match is not None: self.type = match.group(1)

  def __str__(self):
    """Override normal string representation with one which includes all the
    attributes of a report error.
    """
    return ReportError._ERROR_TEMPLATE % self.__dict__


class ConnectionError(Error):

  """URL connection error class."""

  def __init__(self, url, message, http_error_code=None):
    """Initializes the Error with a connection specific error message."""

    Error.__init__(self, 'URL connection error:\n' + message +
                   '\nwhile attempting to connect to: ' + url)
    self.http_error_code = http_error_code


class ReportRunner:

  """This class contains the logic to generate a report from the Reporting API
  web service.
  """

  _AUTH_URL = 'https://www.google.com/accounts/ClientLogin'
  _REPORTING_URL = ('https://www.google.com'
                    '/hosted/services/v1.0/reports/ReportingData')

  def __init__(self):
    """Construct an instance of the report runner."""

    self.admin_email = None
    self.admin_password = None
    self.domain = None
    self.token = None

  def __PostUrl(self, url, data):
    """Post data to a URL.

    Args:
      url: URL to post to.
      data: data to post

    Raises:
      ConnectionError: When a connection error occurs or an HTTP response
        error code is returned.
    """
    try:
      return urllib2.urlopen(url, data).read()
    except urllib2.HTTPError, e:
      raise ConnectionError(ReportRunner._AUTH_URL,
                            'HTTP Response Code: %i' % e.code, e.code)
    except urllib2.URLError, e:
      raise ConnectionError(ReportRunner._AUTH_URL, e.reason)

  def Login(self):
    """Get an authorization token from the Auth URL web service.

    This authorization token is cached in the ReportRunner instance.  If a new
    token is needed, for example if the token is 24 hours old, then call this
    method again to get a new token.

    Raises:
      ConnectionError: When a connection error occurs and in particular
        when the credentials are incorrect.
      LoginError: When authentication service does not return a SID token.
    """
    auth_request = urllib.urlencode({
        'accountType': 'HOSTED',
        'Email':       self.admin_email,
        'Passwd':      self.admin_password})
    try:
      auth_response = self.__PostUrl(ReportRunner._AUTH_URL, auth_request)
    except ConnectionError, error:
      if error.http_error_code == 403:
        raise LoginError('Authentication failure')
      else:
        raise

    for line in auth_response.split('\n'):
      (name, value) = line.split('=', 2)
      if name == 'SID':
        self.token = value
        return
    raise LoginError('Unable to get SID token from ' + ReportRunner._AUTH_URL)

  def GetReportData(self, report_request):
    """Get the report data response from the Reporting API web service.

    Args:
      report_request: Reporting API request.

    Returns:
      Report data response as a string.
    """
    report_response = self.__PostUrl(ReportRunner._REPORTING_URL,
                                     report_request.ToXml())
    if report_response is not None and report_response.startswith('<?xml'):
      report_error = ReportError()
      report_error.FromXml(report_response)
      raise report_error
    return report_response
