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
import logging
import re
import socket
import sys
import testing.config
import unittest

class MockLogRecord(object):
  """Defines a super-simple mock LogRecord, with only the fields required
  by the SmartSMTPHandler tests."""
  def __init__(self):
    self.created = 1167608563
    self.exc_info = None
    self.message = "line1\nline2\nline3"
    self.msecs = 0

  def getMessage(self):
    return self.message

def ErrorHandlerRaise(record):
  """Re-raises the last exception. Used as replacement error handler for the
  unit tests of SmartSMTPHandler."""
  raise

class TestSmartSMTPHandler(unittest.TestCase):
  def setUp(self):
    self.config = testing.config.MockConfig()
    self.handler = logger.SmartSMTPHandler("...", "foo@example.com",
                                           "foo@example.com", "example.com", 42)
    self.handler.setFormatter(logging.Formatter(None, '%Y-%m-%d %H:%M:%S'))
    self.handler.handleError = ErrorHandlerRaise
    self.record = MockLogRecord()

  def testAddedRecords(self):
    self.handler._PrepareRecord(self.record)
    self.assertEquals(self.record.asctime, "2007-01-01 00:42:43")
    self.assertEquals(self.record.domain, "example.com")
    self.assertEquals(self.record.message, "line1\nline2\nline3")
    self.assertEquals(self.record.spmessage, "  line2\n  line3")
    self.assertEquals(self.record.subject, "line1")

  def testGetSubject(self):
    self.assertEquals(self.handler.getSubject(self.record),
      "[gappsd-example.com] line1")

  def testFormat(self):
    record = self.handler.format(self.record)
    self.assertTrue(re.match(
      "Host: [a-z._-]+\nDate: 2007-01-01 00:42:43\n"
      "Message: line1\nDetails:\n  line2\n  line3", record), record)

  def testRateLimiter(self):
    """Tests the rate-limiter by sending two times the same message (the first
    one should not be rate-limited, and hence should it raises a Socket error
    due to the SMTP server name (0.0.0.0). Also test the timeout, and
    the correct emitting of other messages."""

    # First message -> pass.
    self.assertRaises(socket.error, self.handler.emit, self.record)

    # Second message -> ignored.
    self.handler.emit(self.record)

    # Third message, after timeout -> pass.
    self.handler._subjects[self.handler.getSubject(self.record)] = \
      datetime.datetime.now() - datetime.timedelta(0, 43)
    self.assertRaises(socket.error, self.handler.emit, self.record)

    # Different message -> pass.
    record_b = MockLogRecord()
    record_b.message = "line2\nline1"
    self.assertRaises(socket.error, self.handler.emit, record_b)
