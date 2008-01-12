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

import gappsd.job as job
import testing.config
import testing.database
import unittest

# TODO: extend the MockJob
class MockJob(job.Job):
  """Dummy implementation of a Job."""

  def __init__(self, value):
    self.value = value


class TestJobRegistry(unittest.TestCase):
  def setUp(self):
    self.registry = job.JobRegistry()

  def testRegistry(self):
    self.registry.register('foo', MockJob)
    mock_job = self.registry.instantiate('foo', 42)

    self.assertEquals(type(mock_job), MockJob)
    self.assertEquals(mock_job.value, 42)


class TestJob(unittest.TestCase):
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
    self.config = testing.config.MockConfig()
    self.sql = testing.database.MockSQL()
    self.sql.update_result = None

  # Instantiation tests.
  def testValueMissingDict(self):
    self.assertRaises(job.JobContentError, job.Job, \
                      self.config, self.sql, self._NO_QID_DICT)

  def testInvalidJsonDict(self):
    self.assertRaises(job.JobContentError, job.Job, \
                      self.config, self.sql, self._BADJSON_DICT)

  # Status change tests.
  def testStatusIdleOrActive(self):
    j = job.Job(self.config, self.sql, self._VALID_DICT)
    self.assertRaises(job.JobActionError, j.update, job.Job.STATUS_IDLE)
    self.assertRaises(job.JobActionError, j.update, job.Job.STATUS_ACTIVE)
  
  def testStatusSoftfail(self):
    j_soft = job.Job(self.config, self.sql, self._VALID_DICT)
    j_hard = job.Job(self.config, self.sql, self._HARDFAIL_DICT)
    
    j_soft.update(job.Job.STATUS_SOFTFAIL, "foo")
    self.assertEqual(self.sql.update_where["q_id"], j_soft._data['queue_id'])
    self.assertEqual(self.sql.update_values["p_status"],
                     job.Job.STATUS_SOFTFAIL)
    self.assertEqual(self.sql.update_values["r_softfail_count"], 2)
    self.assertEqual(self.sql.update_values["r_result"], "foo")
    
    j_hard.update(job.Job.STATUS_SOFTFAIL, "foo")
    self.assertEqual(self.sql.update_where["q_id"], j_hard._data['queue_id'])
    self.assertEqual(self.sql.update_values["p_status"],
                     job.Job.STATUS_HARDFAIL)
    self.assertEqual(self.sql.update_values["r_softfail_count"], 4)
    self.assertEqual(self.sql.update_values["r_result"],
                     "foo [softfail threshold reached]")

  def testStatusSuccessOrFailure(self):
    j_success = job.Job(self.config, self.sql, self._VALID_DICT)
    j_fail = job.Job(self.config, self.sql, self._VALID_DICT)

    j_success.update(job.Job.STATUS_SUCCESS, "foo")
    self.assertEqual(self.sql.update_where["q_id"], j_success._data['queue_id'])
    self.assertEqual(self.sql.update_values["p_status"],
                     job.Job.STATUS_SUCCESS)

    j_fail.update(job.Job.STATUS_HARDFAIL, "bar")
    self.assertEqual(self.sql.update_values["p_status"],
                     job.Job.STATUS_HARDFAIL)
    self.assertEqual(self.sql.update_values["r_result"], "bar")
