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

import gappsd.config
import ConfigParser
import unittest

class TestConfig(unittest.TestCase):
  def testUnparseableFile(self):
    self.assertRaises(ConfigParser.ParsingError,
                      gappsd.config.Config,
                      "testdata/config-unparseable.conf")

  def testMissingValue(self):
    self.assertRaises(gappsd.config.MissingValueError,
                      gappsd.config.Config,
                      "testdata/config-missingvalue.conf")

  def testUnavailableStringOption(self):
    config = gappsd.config.Config("testdata/config-valid.conf")
    self.assertRaises(KeyError, config.getString, "gappsd.queue-min-delay")

  def testUnavailableIntOption(self):
    config = gappsd.config.Config("testdata/config-valid.conf")
    self.assertRaises(KeyError, config.getInt, "gapps.domain")

  def testPositive(self):
    config = gappsd.config.Config("testdata/config-valid.conf")
    self.assertEqual(config.getString("mysql.hostname"), "MH")
    self.assertEqual(config.getString("gapps.domain"), "GD")
    self.assertEqual(config.getInt("gappsd.queue-min-delay"), 4)
    self.assertEqual(config.getInt("gappsd.queue-delay-normal"), 10)
