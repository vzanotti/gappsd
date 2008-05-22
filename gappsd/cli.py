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

"""Command line tool for the GoogleApps Daemon project.

Provides a way to execute jobs that the normal gappsd will refuse to execute
(mainly jobs which involve changes to administrators accounts, or privileged
actions such as account deletion)."""

# TODO(vzanotti): LogOut on authentication tokens.

import getpass
import config, database, logger, queue
import job, provisioning, reporting

class CliQueue(object):
  """TODO"""

  def __init__(self, config, sql):
    self._config = config
    self._sql = sql
    self._jobs = {}
    self._queue = queue.Queue(config, sql)

  def Jobs(self):
    return self._jobs

  def Update(self):
    """TODO"""

    sql_query = "SELECT %s FROM gapps_queue WHERE %s ORDER BY q_id LIMIT 1" % \
      (queue.Queue._JOB_SELECT_CLAUSE,
       queue.Queue._ACTIVE_JOBS_WHERE_CLAUSE_ADMIN)
    result = self._sql.Query(sql_query)

    self._jobs = {}
    for row in result:
      try:
        j = job.job_registry.Instantiate(row["j_type"],
                                        self._config, self._sql, row)
        self._jobs[str(j.id())] = j
      except job.JobError, message:
        job.Job.MarkFailed(self._sql, result[0]["q_id"],
                          "Job instantiation error: %s" % (message,))
        logger.info("Failed to instantiate job %d: %s" % \
          (result[0]["q_id"], message))

  def ProcessJob(self, job_number):
    """TODO"""

    job = self._jobs[job_number]
    self._queue._ProcessJob(job)

class Cli(object):
  """TODO"""

  def __init__(self, config_file, admin_email):
    """Initializes the CLI using default parameters from the config files, and
    specialized parameters for the admin credentials."""

    (username, domain) = admin_email.split("@")
    self._config = config.Config(config_file)
    self._config.set('gapps.admin-api-username', username)
    self._config.set('gapps.admin-api-password',
                     getpass.getpass("%s's password: " % admin_email))
    self._config.set('gappsd.admin-only-jobs', True)
    self._sql = database.SQL(self._config)
    self._queue = CliQueue(self._config, self._sql)
    logger.InitializeLogging(None, True)

  def ListJobs(self, jobs):
    if not len(jobs):
      print("No admin request left, terminating.")
      return False

    print("Please choose a job to process: ")
    for (job_id, job) in jobs.items():
      print("(%s) %s" % (job_id, job))
    return True

  def PrintJob(self, jobs, job_number):
    if not job_number in jobs:
      print("Unknown job !\n")
      return False

    job = jobs[job_number]
    print job.__longstr__()
    return True

  def Run(self):
    """TODO"""

    while True:
      self._queue.Update()
      jobs = self._queue.Jobs()
      self.ListJobs(jobs)
      if not len(jobs):
        break

      # Asks for job # to process.
      job = raw_input("Job ? ")
      if not job:
        break
      if not self.PrintJob(jobs, job):
        continue

      # Confirms the admin's intention to execute the job.
      confirm = raw_input("Confirm execution of this job ? (n/y) ")
      if len(confirm) == 0 or not confirm[0] == 'y':
        print("Aborting ...")
        continue

      # Processes the job.
      print("")
      self._queue.ProcessJob(job)
      print("")
