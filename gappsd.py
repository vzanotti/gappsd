#!/usr/bin/python2.5
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

"""TODO"""

import optparse
import gappsd.daemon

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--config-file", action="store", dest="config_file")
  parser.add_option("-v", "--verbose", action="store_true", dest="verbose")
  (options, args) = parser.parse_args()

  if options.config_file is None:
    print("Error: option --config-file is mandatory.")
    exit(1)
  gappsd.daemon.Daemon(options.config_file, options.verbose or False).Run()

if __name__ == '__main__':
  main()
