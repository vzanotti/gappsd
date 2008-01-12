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

import gappsd.config as config
import ConfigParser
import unittest

class MockConfig(config.Config):
  """Provides an always valid configuration object for other tests."""

  def __init__(self):
    config.Config.__init__(self, "testdata/config-valid.conf")


class TestConfig(unittest.TestCase):
  def setUp(self):
    self.config = config.Config("testdata/config-valid.conf")

  def testUnparseableFile(self):
    self.assertRaises(ConfigParser.ParsingError,
                      config.Config,
                      "testdata/config-unparseable.conf")

  def testMissingValue(self):
    self.assertRaises(config.MissingValueError,
                      config.Config,
                      "testdata/config-missingvalue.conf")

  def testUnavailableStringOption(self):
    self.assertRaises(KeyError, self.config.getString, "gappsd.queue-min-delay")

  def testUnavailableIntOption(self):
    self.assertRaises(KeyError, self.config.getInt, "gapps.domain")

  def testPositive(self):
    self.assertEquals(self.config.getString("mysql.hostname"), "MH")
    self.assertEquals(self.config.getString("gapps.domain"), "GD")
    self.assertEquals(self.config.getInt("gappsd.queue-min-delay"), 4)
    self.assertEquals(self.config.getInt("gappsd.queue-delay-normal"), 10)

  def testConfigUpdate(self):
    self.config.setString("a", "b")
    self.assertEquals(self.config.getString("a"), "b")
    self.config.setInt("a", "42")
    self.assertEquals(self.config.getInt("a"), 42)
