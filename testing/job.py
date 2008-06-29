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

import gappsd.database as database
import gappsd.job as job
import testing.config
import testing.database
import mox, unittest


def RegisterMockedJob(mocked_job):
  """Adds a specific *object* mocked job in the registry; used for tests to make
  sure the instantiated job is a known one."""

  job.job_registry.Register('mock', lambda x, y, z: mocked_job)

class DummyJob(object):
  """Dummy job for the JobRegistry test."""
  pass

class TestJobRegistry(unittest.TestCase):
  def setUp(self):
    self.registry = job.JobRegistry()

  def testRegistry(self):
    self.registry.Register('foo', DummyJob)
    mock_job = self.registry.Instantiate('foo')
    self.assertEquals(type(mock_job), DummyJob)


class TestJob(mox.MoxTestBase):
  _VALID_DICT = {
    "q_id": 42, "p_status": "active", "p_entry_date": 1200043549,
    "p_start_date": 1200043559, "r_softfail_count": 1,
    "r_softfail_date": 1200043259, "j_type": "u_create", "j_parameters": "{}"
  }
  _HARDFAIL_DICT = _VALID_DICT.copy()
  _HARDFAIL_DICT.update({"q_id": 43, "r_softfail_count": 3})
  _BADJSON_DICT = _VALID_DICT.copy()
  _BADJSON_DICT.update({"j_parameters": ""})
  _NO_QID_DICT = _VALID_DICT.copy()
  _NO_QID_DICT.pop("q_id")

  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.config = testing.config.MockConfig()
    self.sql = self.mox.CreateMock(database.SQL)

  # Instantiation tests.
  def testValueMissingDict(self):
    self.assertRaises(job.JobContentError, job.Job, \
                      self.config, self.sql, self._NO_QID_DICT)

  def testInvalidJsonDict(self):
    self.assertRaises(job.JobContentError, job.Job, \
                      self.config, self.sql, self._BADJSON_DICT)

  # Status change tests.
  def testMarkFailed(self):
    self.sql.Update('gapps_queue',
                    mox.And(mox.ContainsKeyValue('p_status', job.Job.STATUS_HARDFAIL),
                            mox.ContainsKeyValue('r_result', 'blah')),
                    mox.ContainsKeyValue('q_id', 42))
    self.mox.ReplayAll()

    job.Job.MarkFailed(self.sql, 42, "blah")

  def testMarkAdmin(self):
    self.sql.Update('gapps_queue',
                    mox.And(mox.ContainsKeyValue('p_status', job.Job.STATUS_IDLE),
                            mox.ContainsKeyValue('p_admin_request', True)),
                    mox.IgnoreArg())
    self.mox.ReplayAll()

    j = job.Job(self.config, self.sql, self._VALID_DICT)
    j.MarkAdmin()

  def testMarkActive(self):
    self.sql.Update('gapps_queue',
                    mox.ContainsKeyValue('p_status', job.Job.STATUS_ACTIVE),
                    mox.IgnoreArg())
    self.mox.ReplayAll()

    j = job.Job(self.config, self.sql, self._VALID_DICT)
    j.MarkActive()
    self.assertEquals(j._data["p_status"], job.Job.STATUS_ACTIVE)

  def testStatusIdleOrActive(self):
    j = job.Job(self.config, self.sql, self._VALID_DICT)
    self.assertRaises(job.JobActionError, j.Update, job.Job.STATUS_IDLE)
    self.assertRaises(job.JobActionError, j.Update, job.Job.STATUS_ACTIVE)

  def testStatusSoftfail(self):
    j_soft = job.Job(self.config, self.sql, self._VALID_DICT)
    j_hard = job.Job(self.config, self.sql, self._HARDFAIL_DICT)

    self.sql.Update('gapps_queue',
                    mox.And(mox.ContainsKeyValue('p_status', job.Job.STATUS_SOFTFAIL),
                    mox.And(mox.ContainsKeyValue('r_softfail_count', 2),
                            mox.ContainsKeyValue('r_result', 'foo'))),
                    mox.IgnoreArg())
    self.mox.ReplayAll()
    j_soft.Update(job.Job.STATUS_SOFTFAIL, "foo")
    self.assertEqual(j_soft._data['q_id'], 42)
    self.mox.ResetAll()

    self.sql.Update('gapps_queue',
                    mox.And(mox.ContainsKeyValue('p_status', job.Job.STATUS_HARDFAIL),
                    mox.And(mox.ContainsKeyValue('r_softfail_count', 4),
                            mox.ContainsKeyValue('r_result', 'foo [softfail threshold reached]'))),
                    mox.IgnoreArg())
    self.mox.ReplayAll()
    j_hard.Update(job.Job.STATUS_SOFTFAIL, "foo")
    self.assertEqual(j_soft._data['q_id'], 42)
    self.mox.ResetAll()

  def testStatusSuccessOrFailure(self):
    j_success = job.Job(self.config, self.sql, self._VALID_DICT)
    j_fail = job.Job(self.config, self.sql, self._VALID_DICT)

    self.sql.Update(mox.IgnoreArg(),
                    mox.ContainsKeyValue('p_status', job.Job.STATUS_SUCCESS),
                    mox.IgnoreArg())
    self.mox.ReplayAll()
    j_success.Update(job.Job.STATUS_SUCCESS, "foo")
    self.assertEqual(j_success._data['q_id'], 42)
    self.mox.ResetAll()

    self.sql.Update(mox.IgnoreArg(),
                    mox.And(mox.ContainsKeyValue('p_status', job.Job.STATUS_HARDFAIL),
                            mox.ContainsKeyValue('r_result', 'bar')),
                    mox.IgnoreArg())
    self.mox.ReplayAll()
    j_fail.Update(job.Job.STATUS_HARDFAIL, "bar")
    self.assertEquals(j_fail._data["p_status"], job.Job.STATUS_HARDFAIL)
    self.mox.ResetAll()
