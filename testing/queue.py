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
import gappsd.database as database
import gappsd.job as job
import gappsd.logger as logger
import gappsd.queue as queue
import testing.config
import mox, unittest

class TestCreateQueueJob(mox.MoxTestBase):
  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.sql = self.mox.CreateMock(database.SQL)

  def testCreateQueueJob(self):
    self.sql.Insert('gapps_queue', {
      "j_type": "u_sync",
      "j_parameters": "[{}, {\"blih\": 1}]",
      "p_priority": "normal",
      "p_entry_date": "2007-01-01 01:00:00",
      "p_notbefore_date": "2007-01-01 01:00:00",
    })
    self.mox.ReplayAll()

    queue.CreateQueueJob(self.sql, 'u_sync', [{}, {"blih": 1}],
                         p_entry_date=datetime.datetime(2007, 1, 1, 1))

class TestQueue(mox.MoxTestBase):
  _VALID_JOB_DICT = {
    "q_id": 1, "p_status": "idle", "p_entry_date": 42, "p_start_date": 42,
    "r_softfail_count": 0, "r_softfail_date": None, "j_parameters": None
  }

  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.config = testing.config.MockConfig()
    self.sql = self.mox.CreateMock(database.SQL)
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
    self.sql.Query(mox.IgnoreArg()).AndReturn([
      {"p_priority": "immediate", "count": 42},
      {"p_priority": "normal", "count": 69},
      {"p_priority": "offline", "count": 666},
    ])
    self.mox.ReplayAll()

    self.assertEquals(self.queue._GetJobCounts(),
                      {"immediate": 42, "normal": 69, "offline": 666})

  def testGetJobFromQueue(self):
    kTestJob = self.mox.CreateMock(job.Job)
    testing.job.RegisterMockedJob(kTestJob)

    # Tests a failed Job Retrieval.
    self.sql.Query(mox.IgnoreArg(), mox.IgnoreArg()).AndReturn(())
    self.mox.ReplayAll()
    self.assertEquals(self.queue._GetJobFromQueue('offline'), None)
    self.mox.ResetAll()

    # Tests an invalid job retrieval.
    self._VALID_JOB_DICT['j_type'] = 'foo'
    self.sql.Query(mox.IgnoreArg(), mox.IgnoreArg()).AndReturn([self._VALID_JOB_DICT])
    self.sql.Update(mox.IgnoreArg(),
                    mox.And(mox.ContainsKeyValue('p_status', 'hardfail'),
                            mox.ContainsKeyValue('r_result', "Job instantiation error: Job 'foo' is undefined.")),
                    mox.IgnoreArg())
    self.mox.ReplayAll()
    self.assertEquals(self.queue._GetJobFromQueue('offline'), None)
    self.mox.ResetAll()

    # Tests a successful job retrieval.
    self._VALID_JOB_DICT['j_type'] = 'mock'
    self.sql.Query(mox.IgnoreArg(), mox.IgnoreArg()).AndReturn([self._VALID_JOB_DICT])
    kTestJob.MarkActive()
    self.mox.ReplayAll()
    j = self.queue._GetJobFromQueue('offline')
    self.mox.ResetAll()

  def testProcessJob(self):
    kTestJob = self.mox.CreateMock(job.Job)

    # Tests a successful job.
    kTestJob.status().AndReturn(('active', 0))
    kTestJob.Run()
    kTestJob.status().AndReturn(('active', 0))
    kTestJob.Update('success')
    self.mox.ReplayAll()
    self.queue._ProcessJob(kTestJob)
    self.mox.ResetAll()

    # Tests a TransientError-raising job.
    kTestJob.status().AndReturn(('active', 0))
    kTestJob.Run().AndRaise(logger.TransientError)
    kTestJob.Update('softfail', mox.IsA(logger.TransientError))
    self.mox.ReplayAll()
    self.queue._ProcessJob(kTestJob)
    self.mox.ResetAll()

    # Tests a PermanentError-raising job.
    kTestJob.status().AndReturn(('active', 0))
    kTestJob.Run().AndRaise(logger.PermanentError)
    kTestJob.Update('hardfail', mox.IsA(logger.PermanentError))
    self.mox.ReplayAll()
    self.queue._ProcessJob(kTestJob)
    self.mox.ResetAll()

  def testProcessNextJob(self):
    self.mox.StubOutWithMock(self.queue, "_GetJobCounts")
    self.mox.StubOutWithMock(self.queue, "_GetJobFromQueue")
    self.mox.StubOutWithMock(self.queue, "_ProcessJob")
    self.queue._GetJobCounts().AndReturn({
      "immediate": 1, "normal": 0, "offline": 1})
    self.queue._GetJobFromQueue('immediate').AndReturn('immediate_job')
    self.queue._ProcessJob('immediate_job')
    self.queue._GetJobFromQueue('offline').AndReturn('offline_job')
    self.queue._ProcessJob('offline_job')
    self.mox.ReplayAll()

    self.queue._ProcessNextJob()
    self.assertEquals(self.queue._job_counts["immediate"], 1)

  def testAddTransientError(self):
    # Raises an exception to make sure sys.exc_info returns something.
    try:
      this_variable_doesnt_exist
    except:
      pass

    self.queue._AddTransientError(testing.job.DummyJob(), "message")
    self.assertEquals(len(self.queue._transient_errors), 1)

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
