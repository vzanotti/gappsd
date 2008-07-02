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

"""Maintains the GApps job queue in a proper state, by removing old unclaimed
terminated jobs, and by adding the reporting update jobs.
It should be called daily, in a cron, so as to ensure proper update of reporting
data.

Usage:
  queue-cleaner --config-file /path/to/config/file
"""

# Sets up the python path for 'gappsd' modules inclusion.
import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import gappsd.config, gappsd.database
import optparse

class QueueCleaner(object):
  # Delay before removing a finished job from the queue, in days.
  _FAILED_JOBS_DELAY = 7
  _SUCCESSFUL_JOBS_DELAY = 1

  def __init__(self, config_file):
    self._config = gappsd.config.Config(config_file)
    self._sql = gappsd.database.SQL(self._config)

  def CleanQueue(self):
    """Executes queue clean-up / reporting jobs."""

    self.RemoveFailedJobs()
    self.RemoveSuccessfulJobs()
    if not self.HasReportingJobs():
      self.AddReportingJobs()

  def RemoveFailedJobs(self):
    """Removes old failed unclaimed jobs."""

    self._RemoveJobs('hardfail', self._FAILED_JOBS_DELAY)

  def RemoveSuccessfulJobs(self):
    """Removes old successful unclaimed jobs."""

    self._RemoveJobs('success', self._SUCCESSFUL_JOBS_DELAY)

  def _RemoveJobs(self, status, delay):
    sql_query = \
        """DELETE FROM gapps_queue
            WHERE p_status = %s AND
                  p_end_date < DATE_SUB(NOW(), INTERVAL %s DAY)"""
    self._sql.Execute(sql_query, (status, delay))

  def HasReportingJobs(self):
    """Indicates whether active reporting update jobs are present or not in the
    job queue."""

    count = self._sql.Query( \
        """SELECT COUNT(*) AS count
             FROM gapps_queue
            WHERE j_type IN ('r_accounts', 'r_activity') AND p_status = 'idle'""")
    return count[0]["count"]

  def AddReportingJobs(self):
    """Adds reporting update jobs to the queue."""

    self._sql.Execute( \
        """INSERT INTO gapps_queue
                   SET p_entry_date = NOW(),
                       p_notbefore_date = NOW(),
                       p_status = 'idle',
                       p_priority = 'offline',
                       j_type = 'r_accounts',
                       j_parameters = NULL""")
    self._sql.Execute( \
        """INSERT INTO gapps_queue
                   SET p_entry_date = NOW(),
                       p_notbefore_date = NOW(),
                       p_status = 'idle',
                       p_priority = 'offline',
                       j_type = 'r_activity',
                       j_parameters = NULL""")


if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option("-c", "--config-file", action="store", dest="config_file")
  (options, args) = parser.parse_args()

  if options.config_file is None:
    print("Error: options --config-file is mandatory.")
    exit(1)

  cleaner = QueueCleaner(options.config_file)
  cleaner.CleanQueue()
