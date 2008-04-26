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
import gappsd.job as job
import gappsd.logger as logger
import gappsd.queue as queue
import testing.config
import testing.database
import unittest

class TestCreateQueueJob(unittest.TestCase):
  def setUp(self):
    self.sql = testing.database.MockSQL()

  def testCreateQueueJob(self):
    self.sql.insert_result = None
    queue.CreateQueueJob(self.sql, 'u_sync', [{}, {"blih": 1}],
                         p_entry_date=datetime.datetime(2007, 1, 1, 1))
    self.assertEquals(self.sql.insert_values, {
      "j_type": "u_sync",
      "j_parameters": "[{}, {\"blih\": 1}]",
      "p_priority": "normal",
      "p_entry_date": "2007-01-01 01:00:00",
      "p_notbefore_date": "2007-01-01 01:00:00",
    })

class TestQueue(unittest.TestCase):
  _VALID_JOB_DICT = {
    "q_id": 1, "p_status": "idle", "p_entry_date": 42, "p_start_date": 42,
    "r_softfail_count": 0, "r_softfail_date": None, "j_type": "foo",
    "j_parameters": None
  }

  def setUp(self):
    self.config = testing.config.MockConfig()
    self.sql = testing.database.MockSQL()
    self.queue = queue.Queue(self.config, self.sql)

  def testCanWarnForOverflow(self):
    self.assertEquals(self.queue._CanWarnForQueueOverflow('normal'), True)
    self.queue._last_overflow_warning['normal'] = \
      datetime.datetime.now() - datetime.timedelta(0, 3700)
    self.assertEquals(self.queue._CanWarnForQueueOverflow('normal'), True)
    self.queue._last_overflow_warning['normal'] = \
      datetime.datetime.now() - datetime.timedelta(0, 3500)
    self.assertEquals(self.queue._CanWarnForQueueOverflow('normal'), False)

    self.assertEquals(self.queue._CanWarnForQueueOverflow('offline'), True)
    self.assertEquals(self.queue._CanWarnForQueueOverflow('immediate'), True)

  def testWarnForOverflow(self):
    self.queue._WarnForQueueOverflow('normal', 1)
    self.queue._WarnForQueueOverflow('normal', 1)

  def testGetCurrentQueueDelays(self):
    job_counts = {"immediate": 86400, "normal": 42, "offline": 5760}
    delays = self.queue._GetCurrentQueueDelays(job_counts)
    self.assertEquals(delays['immediate'], 4)
    self.assertEquals(delays['normal'], 10)
    self.assertEquals(delays['offline'], 15)

  def testCanProcessFromQueue(self):
    self.assertEquals(self.queue._CanProcessFromQueue('normal', 10), True)

  def testGetNextPriorityQueue(self):
    job_counts = {"immediate": 10, "normal": 10, "offline": 10}
    queues = [q for q in self.queue._GetNextPriorityQueue(job_counts)]
    self.assertEquals(queues, ['immediate', 'normal', 'offline'])

    self.queue._last_jobs['normal'] -= datetime.timedelta(0, 10)
    queues = [q for q in self.queue._GetNextPriorityQueue(job_counts)]
    self.assertEquals(queues, ['normal'])

    queues = [q for q in self.queue._GetNextPriorityQueue(job_counts)]
    self.assertEquals(queues, [])

  def testGetJobCounts(self):
    self.sql.query_result = [
      {"p_priority": "immediate", "count": 42},
      {"p_priority": "normal", "count": 69},
      {"p_priority": "offline", "count": 666},
    ]
    self.assertEquals(self.queue._GetJobCounts(),
                      {"immediate": 42, "normal": 69, "offline": 666})

  def testGetJobFromQueue(self):
    # Tests a failed Job Retrieval.
    self.sql.query_result = ()
    self.assertEquals(self.queue._GetJobFromQueue('offline'), None)

    # Tests an invalid job retrieval.
    self.sql.query_result = [self._VALID_JOB_DICT]
    self.sql.update_result = None
    self.assertEquals(self.queue._GetJobFromQueue('offline'), None)
    self.assertEquals(self.sql.update_values["p_status"], "hardfail")
    self.assertEquals(self.sql.update_values["r_result"],
                      "Job instantiation error: Job 'foo' is undefined.")

    # Tests a successful job retrieval.
    self.sql.query_result = [{
      "q_id": 1, "p_status": "idle", "p_entry_date": 42, "p_start_date": 42,
      "r_softfail_count": 0, "r_softfail_date": None, "j_type": "mock",
      "j_parameters": None
    }]
    j = self.queue._GetJobFromQueue('offline')
    self.assertEquals(j._data["p_entry_date"], 42)

  def testProcessJob(self):
    # Tests a successful job.
    j = testing.job.MockJob(None, None, self._VALID_JOB_DICT)
    j.MarkActive()
    self.queue._ProcessJob(j)
    self.assertEquals(j.update_status, job.Job.STATUS_SUCCESS)

    # Tests a TransientError-raising job.
    j = testing.job.MockJob(None, None, self._VALID_JOB_DICT)
    j.run_result = logger.TransientError()
    j.MarkActive()
    self.queue._ProcessJob(j)
    self.assertEquals(j.update_status, job.Job.STATUS_SOFTFAIL)

    # Tests a PermanentError-raising job.
    j = testing.job.MockJob(None, None, self._VALID_JOB_DICT)
    j.run_result = logger.PermanentError()
    j.MarkActive()
    self.queue._ProcessJob(j)
    self.assertEquals(j.update_status, job.Job.STATUS_HARDFAIL)

  def testProcessNextJob(self):
    self.job_requested = {}
    self.job_processed = []
    def mockGetJobCounts():
      return {"immediate": 1, "normal": 0, "offline": 1}
    def mockGetJobFromQueue(queue):
      self.job_requested[queue] = True
      return queue
    def mockProcessJob(job):
      self.job_processed.append(job)

    self.queue._GetJobCounts = mockGetJobCounts
    self.queue._GetJobFromQueue = mockGetJobFromQueue
    self.queue._ProcessJob = mockProcessJob

    self.queue._ProcessNextJob()
    self.assertEquals(self.queue._job_counts["immediate"], 1)
    self.assertEquals(self.job_processed[0], "immediate")
    self.assertEquals(self.job_processed[1], "offline")

  def testAddTransientError(self):
    try:
      this_variable_doesnt_exist
    except:
      pass

    j = testing.job.MockJob(None, None, self._VALID_JOB_DICT)
    self.queue._AddTransientError(j, "message")

  def testCheckTransientErrors(self):
    self.queue._CREDENTIAL_ERRORS_THRESHOLD = 1
    self.queue._TRANSIENT_ERRORS_THRESHOLD = 1

    # Tests the method with only one expired Transient error.
    self.queue._transient_errors = [{
      "date": datetime.datetime.now() - datetime.timedelta(0, 3601),
      "job": "<job>",
      "exc_type": logger.TransientError,
      "message": "<message>"
    }]
    self.queue._CheckTransientErrors()

    # Tests the method with one expired and one valid Transient errors.
    self.queue._transient_errors = [{
      "date": datetime.datetime.now() - datetime.timedelta(0, 3601),
      "job": "<job>",
      "exc_type": logger.TransientError,
      "message": "<message>"
    }]
    self.queue._transient_errors.append({
      "date": datetime.datetime.now(),
      "job": "<job>",
      "exc_type": logger.TransientError,
      "message": "<message>"
    })
    self.assertRaises(logger.TransientError, self.queue._CheckTransientErrors)

    # Tests the method with only one expired Credential error.
    self.queue._transient_errors = [{
      "date": datetime.datetime.now() - datetime.timedelta(0, 3601),
      "job": "<job>",
      "exc_type": logger.CredentialError,
      "message": "<message>"
    }]
    self.queue._CheckTransientErrors()

    # Tests the method with one expired and one valid Credential errors.
    self.queue._transient_errors = [{
      "date": datetime.datetime.now() - datetime.timedelta(0, 3601),
      "job": "<job>",
      "exc_type": logger.CredentialError,
      "message": "<message>"
    }]
    self.queue._transient_errors.append({
      "date": datetime.datetime.now(),
      "job": "<job>",
      "exc_type": logger.CredentialError,
      "message": "<message>"
    })
    self.assertRaises(logger.CredentialError, self.queue._CheckTransientErrors)

  def testRun(self):
    # TODO(vzanotti): Add unittests for this metod.
    pass
