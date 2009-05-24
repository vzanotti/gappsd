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

"""Google Apps API daemon.

Acts as an interface between the Google Apps API (more specifically the
provisionning and reporting APIs), and a random website, by the mean of shared
SQL tables.

More specifically, gappsd maintains a local SQL mirror of Google Apps user
management data (statistical informations as well as per user details), and
executes "jobs" submitted by the website. Those jobs include user accounts
maintenance (creation, update, deletion), and statistics update requests.

Please see tools/queue-cleaner.py for a job queue maintenance tool (which also
automatizes the statistics update).


Two of the main features of gappsd are reliability and security: it makes the
most to ensure that jobs are correctly executed, even in case of Google-side
transient failures. The security is provided in two ways: first, gappsd can be
executed by a dedicated UNIX user (if correctly configured, it prevents the
website from acessing Google Apps credentials stored in gappsd configuration);
second, it refuses to execute jobs that would be dangerous for the Google Apps
domaine (for example, user deletion jobs, and admin accounts password update
jobs can't executed by the gappsd (see gapps-cli.py for details on how to
execute them).

Usage: gappsd.py --config-file <path/to/config.file>
"""

import optparse
import sys
import gappsd.daemon

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--config-file", action="store", dest="config_file")
  parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
  (options, args) = parser.parse_args()

  if options.config_file is None:
    print("Error: option --config-file is mandatory.")
    sys.exit(1)
  gappsd.daemon.Daemon(options.config_file, options.verbose or False).Run()

if __name__ == '__main__':
  main()
