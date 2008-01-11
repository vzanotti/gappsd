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
import simplejson
import sys

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
    job.job_registry.register('u_foo', FooJob)
    
    # Instantiates a job based on its name.
    job.job_registry.instantiate('u_foo', params, ...)
  """
  
  def __init__(self):
    self._job_types = {}

  def register(self, job_type, job_class):
    self._job_types[job_type] = job_class;

  def instantiate(self, job_type, *args):
    try:
      return self._job_types[job_type](*args)
    except KeyError:
      raise JobTypeError, "Job '%s' is undefined." % job_type


class Job(object):
  """Represents a Job extracted from the queue. This class is supposed to be
  subclassed to define per-job behaviour.
  Subclasses just have to redefine the run() method, and to implement a proper
  constructor.
  
  Example usge:
    job = Job(config, queue, dict)
    try:
      job.run()
    except JobActionError:
      job.update(job.STATUS_SOFTFAIL,
                 "JobActionError catched while running the job.")
  """
  
  # Enumeration of valid status (Cf. field p_status in the sql queue table).
  STATUS_IDLE = "idle"
  STATUS_ACTIVE = "active"
  STATUS_SUCCESS = "success"
  STATUS_SOFTFAIL = "softfail"
  STATUS_HARDFAIL = "hardfail"
  
  # List of fields to offer data for. Format:
  #   <new name> : [<old name>, <modifier>]
  _JOB_VALUES = {
    "queue_id": ["q_id", int],
    "status": ["p_status", None],
    "entry_date": ["p_entry_date", datetime.datetime.fromtimestamp],
    "start_date": ["p_start_date", datetime.datetime.fromtimestamp],
    "softfail_count": ["r_softfail_count", int],
    "softfail_date": ["r_softfail_date", datetime.datetime.fromtimestamp],
    "job_type": ["j_type", None],
    "job_parameters": ["j_parameters", simplejson.loads],
  }
  
  def __init__(self, config, queue, job_dict):
    """Initializes the job using values offered by the dictionary. Throws
    a JobContentError if an important entry is missing.
    """
    
    self._config = config
    self._queue = queue
    self._softfail_delay = config.getInt("gappsd.job-softfail-delay")
    self._softfail_threshold = config.getInt("gappsd.job-softfail-threshold")
    
    try:
      for (key, (pname, modifier)) in self._JOB_VALUES.items():
        if modifier == None:
          self.__dict__[key] = job_dict[pname]
        else:
          self.__dict__[key] = modifier(job_dict[pname])
    except KeyError:
      raise JobContentError, \
        "Value of %s wasn't found in the job dictionary." % \
        sys.exc_info()[1]
    except TypeError:
      raise JobContentError, "The job_dict parameter should be a dictionary."
    except ValueError:
      raise JobContentError, \
        "Value of JSON-encoded 'j_parameters' field is invalid (%s)." % \
        sys.exc_info()[1]

  def __str__(self):
    return "Job '%s', queue id %d, created on %s, status '%s'" % \
      (self.job_type, self.queue_id, self.start_date, self.status)

  def update(self, status, message=""):
    """Updates the object status /in/ the queue (using the queue interface
    to manipulate the queue)."""

    values = {}
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if status == self.STATUS_IDLE or status == self.STATUS_ACTIVE:
      # Status "idle" is supposed to be set by the queue feeder (ie. the
      # website), while "active" is only supposed to be updated by the
      # queue manager.
      raise JobActionError, \
        "A job status cannot be set to 'idle' or 'active' mode."

    if status == self.STATUS_SOFTFAIL:
      # On softfail, the r_softfail_* are updated. When the softfail count
      # reach a predefined threshold, the status becomes hardfail.
      self.softfail_count += 1
      if self.softfail_count >= self._softfail_threshold:
        status = self.STATUS_HARDFAIL
        message = "%s [softfail threshold reached]" % message
      
      notbefore = \
        datetime.datetime.now() + datetime.timedelta(0, self._softfail_delay)
      values["p_status"] = status
      values["p_notbefore_date"] = notbefore.strftime("%Y-%m-%d %H:%M:%S")
      values["r_softfail_date"] = now
      values["r_softfail_count"] = self.softfail_count
      values["r_result"] = message

    if status == self.STATUS_SUCCESS or status == self.STATUS_HARDFAIL:
      # On success or on hardfail, the result message is updated, so is the
      # processing end date.
      values["p_status"] = status
      values["p_end_date"] = now
      values["r_result"] = message
    
    if len(values) == 0:
      # Status was set to an unknown value, fail.
      raise JobActionError, "Unknown status %s" % status
    
    self._queue.updateJob(self.queue_id, values)


# Job registry used by external modules to register new job types.
job_registry = JobRegistry()
import provisioning, reporting
