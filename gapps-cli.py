#!/usr/bin/env python
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

"""Command line tool for the GoogleApps Daemon project.

Provides a way to execute jobs that the normal gappsd will refuse to execute
(mainly jobs which involve changes to administrators accounts, or privileged
actions such as account deletion).

Usage:
  gapps-cli.py -c <configfile> -a <admin_email>
"""

import optparse
import sys
import gappsd.cli

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--config-file", action="store", dest="config_file")
  parser.add_option("-a", "--admin-email", action="store", dest="admin_email")
  (options, args) = parser.parse_args()

  if options.admin_email is None:
    print("Error: option --admin-email is mandatory.")
    sys.exit(1)
  if options.config_file is None:
    print("Error: option --config-file is mandatory.")
    sys.exit(1)
  gappsd.cli.Cli(options.config_file, options.admin_email).Run()

if __name__ == '__main__':
  main()
