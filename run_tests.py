#!/usr/bin/env python2.5
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

import logging
import unittest
import testing.account
import testing.config
import testing.daemon
import testing.database
import testing.job
import testing.logger
import testing.provisioning
import testing.queue
import testing.reporting

if __name__ == '__main__':
  logging.root.setLevel(logging.CRITICAL + 1)

  suite = unittest.TestSuite();
  for module in dir(testing):
    suite.addTests(unittest.defaultTestLoader.loadTestsFromModule(
      testing.__dict__[module]
    ))
  unittest.TextTestRunner().run(suite)
