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

"""Defines the base "Job" class used to represent a queued job. Also provides
a JobRegistry to register new implementations of the Job class.
"""

import datetime
import pprint
import simplejson
import sys

import logger

class JobError(Exception):
  """The mother exception of all job-related exceptions."""
  pass

class JobTypeError(JobError):
  """Indicate that the requester job doesn't exist."""
  pass

class JobContentError(JobError):
  """Indicates that the job as an invalid content (for example, it was
  instantiated from an incomplete/bad source."""

class JobActionError(JobError):
  """Indicates that an error occured while updating the status of a job.
  For example the new status is invalid."""


class JobRegistry(object):
  """Holds a register of implementation of the Job class. It is used to
  deserialize jobs stored in the queue. Cf. later the global variable
  "job_registry" which contains the global Job Registry

  Example usage:
    # Registers a new Job type.
    class FooJob(Job):
      pass
    job.job_registry.Register('u_foo', FooJob)

    # Instantiates a job based on its name.
    job.job_registry.Instantiate('u_foo', params, ...)
  """

  def __init__(self):
    self._job_types = {}

  def Register(self, job_type, job_class):
    self._job_types[job_type] = job_class;

  def Instantiate(self, job_type, *args):
    try:
      return self._job_types[job_type](*args)
    except KeyError:
      raise JobTypeError("Job '%s' is undefined." % job_type)


class Job(object):
  """Represents a Job extracted from the queue. This class is supposed to be
  subclassed to define per-job behaviour.
  Subclasses have to:
    * redefine the Run() method (used to run the job)
    * implement a constructor that call Job.__init__ properly

  Example usage:
    job = Job(config, queue, dict)
    try:
      job.Run()
    except JobActionError:
      job.Update(job.STATUS_SOFTFAIL,
                 "JobActionError catched while running the job.")
  """

  # Enumeration of valid status (Cf. field p_status in the sql queue table).
  STATUS_IDLE = "idle"
  STATUS_ACTIVE = "active"
  STATUS_SUCCESS = "success"
  STATUS_SOFTFAIL = "softfail"
  STATUS_HARDFAIL = "hardfail"

  # List of offered data fields to offer data for. Format: <field>: <modifier>
  _DATA_FIELDS = {
    "q_id":             int,
    "p_status":         None,
    "p_entry_date":     datetime.datetime.fromtimestamp,
    "p_start_date":     datetime.datetime.fromtimestamp,
    "r_softfail_count": int,
    "r_softfail_date":  datetime.datetime.fromtimestamp,
    "j_type":           None,
    "j_parameters":     simplejson.loads,
  }
  _DATE_FORMAT = "%Y-%m-%d %H:%M:%S"

  # Indicates if the job will have side effects on Google Apps (for instance, if
  # will change/create/delete user accounts).
  PROP__SIDE_EFFECTS = True

  def __init__(self, config, sql, job_dict):
    """Initializes the job using values offered by the dictionary. Throws
    a JobContentError if an important entry is missing.
    """

    self._config = config
    self._data = {}
    self._softfail_delay = config.get_int("gappsd.job-softfail-delay")
    self._softfail_threshold = config.get_int("gappsd.job-softfail-threshold")
    self._sql = sql

    try:
      for key in self._DATA_FIELDS:
        if self._DATA_FIELDS[key] is None or job_dict[key] is None:
          self._data[key] = job_dict[key]
        else:
          self._data[key] = self._DATA_FIELDS[key](job_dict[key])
    except KeyError:
      raise JobContentError( \
        "No value for field '%s' was found." % sys.exc_info()[1])
    except ValueError:
      raise JobContentError( \
        "Invalid value of JSON field 'j_parameters' (%s)." % sys.exc_info()[1])

    # Alias for easier access to job parameters.
    self._parameters = self._data["j_parameters"]

  def __str__(self):
    return \
      "Job '%s', queue id %d, created on %s, status '%s' (%d soft failures)" % \
      (self._data['j_type'], self._data['q_id'], self._data['p_entry_date'],
       self._data['p_status'], self._data['r_softfail_count'])

  def __longstr__(self):
    return "Job '%s', queue id %d, created on %s:\n %s" % \
      (self._data['j_type'], self._data['q_id'], self._data['p_entry_date'],
       pprint.pformat(self._data['j_parameters'], indent=2))

  def status(self):
    return (self._data['p_status'], self._data['r_softfail_count'])

  def id(self):
    return self._data['q_id']

  def HasSideEffects(self):
    return self.PROP__SIDE_EFFECTS != False

  # Status update methods.
  @staticmethod
  def MarkFailed(sql, queue_id, message):
    """Used to mark a non-instantiable job as such. Updates the job as if it
    was an hard failure."""

    values = {
      "p_status": Job.STATUS_HARDFAIL,
      "p_end_date": datetime.datetime.now().strftime(Job._DATE_FORMAT),
      "r_result": message,
    }
    sql.Update("gapps_queue", values, {"q_id": queue_id})

  def MarkAdmin(self):
    """Marks the job as being an "admin-only" task (eg. administrator's password
    change, account deletion, ..."""

    values = {
      "p_status": self.STATUS_IDLE,
      "p_start_date": None,
      "p_admin_request": True,
    }
    self._data.update(values)
    self._sql.Update("gapps_queue", values, {"q_id": self._data['q_id']})
    logger.critical("Job marked as admin-only",
                    extra={"details": self.__longstr__()})

  def MarkActive(self):
    """Updates the job to the 'currently being processed' status."""

    values = {
      "p_status": self.STATUS_ACTIVE,
      "p_start_date": datetime.datetime.now().strftime(self._DATE_FORMAT),
    }
    self._data.update(values)
    self._sql.Update("gapps_queue", values, {"q_id": self._data['q_id']})

  def Update(self, status, message=""):
    """Updates the object status /in/ the queue (using the queue interface
    to manipulate the queue)."""

    values = {}
    now = datetime.datetime.now().strftime(self._DATE_FORMAT)

    if status in (self.STATUS_IDLE, self.STATUS_ACTIVE):
      # Status "idle" is supposed to be set by the queue feeder (ie. the
      # website), while "active" is only supposed to be updated by the
      # queue manager.
      raise JobActionError( \
        "A job status cannot be set to 'idle' or 'active' mode.")

    if status == self.STATUS_SOFTFAIL:
      # On softfail, the r_softfail_* are updated. When the softfail count
      # reach a predefined threshold, the status becomes hardfail.
      self._data['r_softfail_count'] += 1
      if self._data['r_softfail_count'] >= self._softfail_threshold:
        status = self.STATUS_HARDFAIL
        message = "%s [softfail threshold reached]" % message

      notbefore = \
        datetime.datetime.now() + datetime.timedelta(0, self._softfail_delay)
      values["p_status"] = status
      values["p_notbefore_date"] = notbefore.strftime(self._DATE_FORMAT)
      values["r_softfail_date"] = now
      values["r_softfail_count"] = self._data['r_softfail_count']
      values["r_result"] = message

    if status in (self.STATUS_SUCCESS, self.STATUS_HARDFAIL):
      # On success or on hardfail, the result message is updated, so is the
      # processing end date.
      values["p_status"] = status
      values["p_end_date"] = now
      values["r_result"] = message

    if not len(values):
      # Status was set to an unknown value, fail.
      raise JobActionError("Unknown status %s" % status)

    self._data.update(values)
    self._sql.Update("gapps_queue", values, {"q_id": self._data['q_id']})

  # "Abstract" implementation of the Run method.
  def Run(self):
    raise JobActionError("You can't call Run() on a base Job object.")


# Job registry used by external modules to register new job types.
job_registry = JobRegistry()
