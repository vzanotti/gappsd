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
    config.Config.__init__(self, "testing/data/config-valid.conf")


class TestConfig(unittest.TestCase):
  def setUp(self):
    self.config = config.Config("testing/data/config-valid.conf")

  def testUnparseableFile(self):
    self.assertRaises(ConfigParser.ParsingError,
                      config.Config,
                      "testing/data/config-unparseable.conf")

  def testMissingValue(self):
    self.assertRaises(config.MissingValueError,
                      config.Config,
                      "testing/data/config-missingvalue.conf")

  def testUnavailableOption(self):
    self.assertRaises(KeyError, self.config.get_string, "foo.bar")
    self.assertRaises(KeyError, self.config.get_int, "foo.bar")

  def testPositive(self):
    self.assertEquals(self.config.get_string("mysql.hostname"), "MH")
    self.assertEquals(self.config.get_string("gapps.domain"), "GD")
    self.assertEquals(self.config.get_int("gappsd.queue-min-delay"), 4)
    self.assertEquals(self.config.get_int("gappsd.queue-delay-normal"), 10)

  def testTypeCast(self):
    self.assertRaises(ValueError, self.config.get_int, "gapps.domain")
    self.assertEquals(self.config.get_string("gappsd.queue-min-delay"), "4")

  def testConfigUpdate(self):
    self.config.set("a", "b")
    self.assertEquals(self.config.get_string("a"), "b")
    self.config.set("a", "42")
    self.assertEquals(self.config.get_int("a"), 42)
