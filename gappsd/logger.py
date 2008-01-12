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

"""Logging and error handling helpers of the project. It includes standardized
exceptions, as well as a comprehensive logfile/console/mail logger.

This module shoud be initialized explicitly using InitializeLogging:
  logger.InitializeLogging(<config object>)
"""

import datetime
import logging, logging.handlers

# Exceptions used to indicate the error encountered during job execution.
class PermanentError(Exception):
  """Represents a permanent (data-related) error."""
  pass

class TransientError(Exception):
  """Represents a transient (and non-data-related) error."""
  pass

class CredentialError(TransientError):
  """Reprensents an authentication error, due to bad credentials. Aimed at
  being catched by TransientError filters, which should count CredentialErrors
  and abort the program on repeated errors."""
  pass

# Aliases for the logging function (to allow short info() call when importing *
# from gappsd.logger).
info = logging.info
warning = logging.warning
error = logging.error
critical = logging.critical

# The logging code.
class SmartSMTPHandler(logging.handlers.SMTPHandler):
  """Provides a smart SMTP logger for the project: first it offers well
  formatted emails, and second it ratelimit the mails, based on the email's
  subject (to avoid flooding the admins)."""

  _MAIL_TEMPLATE = \
    "Date: %(asctime)s\n" \
    "Error: %(subject)s\n" \
    "Details:\n%(spmessage)s"
  _SUBJECT_TEMPLATE = "[gappsd-%(domain)s] %(subject)s"

  def __init__(self, mailhost, fromaddr, toaddrs, domain, delay):
    """Initializes the SMTPHandler, and use the @p delay argument as the
    minimal delay between two mails with identical subject."""

    logging.handlers.SMTPHandler.__init__(
      self, mailhost, fromaddr, toaddrs, None)
    self._delay = datetime.timedelta(0, delay)
    self._domain = domain
    self._subjects = {}

  # Formating helpers.
  def prepareRecord(self, record):
    """Prepares the record by adding a few other fields, including the date,
    the domain name, the message, the subject (the first line of the message),
    and the body (the rest of the message, comma-indented)."""

    record.asctime = self.formatter.formatTime(record, self.formatter.datefmt)
    record.domain = self._domain
    record.message = record.getMessage()
    record.spmessage = \
      "\n".join(["  " + line for line in record.message.split('\n')[1:]])
    record.subject = record.message.split('\n')[0]

  def getSubject(self, record):
    """Returns the subject of the log email."""

    if not "subject" in record.__dict__:
      self.prepareRecord(record)
    return self._SUBJECT_TEMPLATE % record.__dict__

  def format(self, record):
    """Returns the formatted version of the log mail's body."""

    if not "subject" in record.__dict__:
      self.prepareRecord(record)

    s = self._MAIL_TEMPLATE % record.__dict__
    if record.exc_info:
      record.exc_text = self.formatException(record.exc_info)
      if record.exc_text:
        s = s + record.exc_text if s[-1] is '\n' else record.exc_text
    return s

  # Rate-limiting emitter.
  def emit(self, record):
    """Acts as a wrapper to the true emit() function; ignores mails catched by
    the rate-limit, and forwards the others."""

    subject = self.getSubject(record)
    if subject in self._subjects and \
       self._subjects[subject] + self._delay > datetime.datetime.now():
      warning("Maillog - mail '%s' rate-limited and ignored", subject)
    else:
      self._subjects[subject] = datetime.datetime.now()
      logging.handlers.SMTPHandler.emit(self, record)

def InitializeLogging(config, alsologtostderr=False):
  """Initializes the logging for the project, using the global configuration."""
  root_handler = logging.root
  root_handler.setLevel(logging.INFO)
  formatter = logging.Formatter(
    '%(asctime)s:%(levelname)s:%(message)s', '%Y-%m-%d %H:%M:%S')

  if alsologtostderr:
    stderr = logging.StreamHandler()
    stderr.setLevel(logging.INFO)
    stderr.setFormatter(formatter)
    root_handler.addHandler(stderr)

  if len(config.getString("gappsd.logfile-name")):
    logfile = logging.handlers.TimedRotatingFileHandler(
      config.getString("gappsd.logfile-name"),
      "midnight",
      config.getInt("gappsd.logfile-rotation"),
      config.getInt("gappsd.logfile-backlog"))
    logfile.setLevel(logging.INFO)
    logfile.setFormatter(formatter)
    root_handler.addHandler(logfile)

  if config.getInt("gappsd.logmail"):
    logmail = SmartSMTPHandler(
      config.getString("gappsd.logmail-smtp"),
      config.getString("gapps.admin-email"),
      config.getString("gapps.admin-email"),
      config.getString("gapps.domain"),
      config.getInt("gappsd.logmail-delay"))
    logmail.setLevel(logging.ERROR)
    logmail.setFormatter(formatter)
    root_handler.addHandler(logmail)
