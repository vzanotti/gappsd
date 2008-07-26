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

"""The Queue module of the GApps daemon."""

import datetime
import pprint
import simplejson
import sys
import time

import database, job
from . import logger
from .logger import PermanentError, TransientError

def CreateQueueJob(sql, j_type, j_parameters={}, p_priority="normal",
                   p_entry_date=None, p_notbefore_date=None):
  """Creates a new queue job, based on the parameters."""

  if p_entry_date is None:
    p_entry_date = datetime.datetime.now()
  if p_notbefore_date is None:
    p_notbefore_date = p_entry_date

  values = {
    "j_type": j_type,
    "j_parameters": simplejson.dumps(j_parameters),
    "p_priority": p_priority,
    "p_entry_date": p_entry_date.strftime("%Y-%m-%d %H:%M:%S"),
    "p_notbefore_date": p_notbefore_date.strftime("%Y-%m-%d %H:%M:%S"),
  }
  sql.Insert("gapps_queue", values)

class Queue(object):
  """Queue manager for the GApps daemon. It handles the complete queue
  processing: it extracts jobs in respect with the scheduling constraints,
  it runs them, and it handles their errors.

  Example usage:
    queue = Queue(config, sql)
    queue.Run() # Doesn't normally return (raises exceptions)
  """

  _PRIORITY_IMMEDIATE = "immediate"
  _PRIORITY_NORMAL = "normal"
  _PRIORITY_OFFLINE = "offline"
  _PRIORITY_ORDER = [_PRIORITY_IMMEDIATE, _PRIORITY_NORMAL, _PRIORITY_OFFLINE]
  _OVERFLOW_WARNING_DELAY = 3600
  _MAX_QUEUE_DELAY = 24 * 3600
  _STATISTICS_DELAY = 1800

  _ACTIVE_JOBS_WHERE_CLAUSE = \
    "p_status IN ('idle', 'active', 'softfail') AND " \
    "p_notbefore_date <= NOW() AND " \
    "p_admin_request IS FALSE AND " \
    "(p_start_date IS NULL OR p_status = 'idle' OR " \
    "DATE_ADD(p_start_date, INTERVAL 90 SECOND) <= NOW())"
  _ACTIVE_JOBS_WHERE_CLAUSE_ADMIN = \
    "p_status IN ('idle', 'active', 'softfail') AND " \
    "p_admin_request IS TRUE AND " \
    "(p_start_date IS NULL OR p_status = 'idle' OR " \
    "DATE_ADD(p_start_date, INTERVAL 90 SECOND) <= NOW())"
  _JOB_SELECT_CLAUSE = \
    "q_id, p_status, UNIX_TIMESTAMP(p_entry_date) AS p_entry_date, " \
    "UNIX_TIMESTAMP(p_start_date) AS p_start_date, r_softfail_count, " \
    "UNIX_TIMESTAMP(r_softfail_date) AS r_softfail_date, j_type, j_parameters"

  _TRANSIENT_ERRORS_VALIDITY = 3600
  _CREDENTIAL_ERRORS_THRESHOLD = 2
  _TRANSIENT_ERRORS_THRESHOLD = 4

  def __init__(self, config, sql):
    self._config = config
    self._sql = sql
    self._min_delay = config.get_int("gappsd.queue-min-delay")
    self._overflow_warning = config.get_int("gappsd.queue-warn-overflow")

    self._delays = {
      self._PRIORITY_IMMEDIATE: config.get_int("gappsd.queue-min-delay"),
      self._PRIORITY_NORMAL: config.get_int("gappsd.queue-delay-normal"),
      self._PRIORITY_OFFLINE: config.get_int("gappsd.queue-delay-offline"),
    }
    self._last_jobs = {
      self._PRIORITY_IMMEDIATE: None,
      self._PRIORITY_NORMAL: None,
      self._PRIORITY_OFFLINE: None,
    }
    self._last_overflow_warning = {
      self._PRIORITY_IMMEDIATE: None,
      self._PRIORITY_NORMAL: None,
      self._PRIORITY_OFFLINE: None,
    }
    self._job_counts = {
      self._PRIORITY_IMMEDIATE: 0,
      self._PRIORITY_NORMAL: 0,
      self._PRIORITY_OFFLINE: 0,
    }
    self._transient_errors = []

  # Queue delay helpers.
  def _CanWarnForQueueOverflow(self, priority):
    """Returns True if it is possible to warn about a queue overflow (in respect
    with the warning delays)."""

    if not self._last_overflow_warning[priority]:
      return True
    next_warning_date = self._last_overflow_warning[priority] + \
      datetime.timedelta(0, self._OVERFLOW_WARNING_DELAY)
    return next_warning_date < datetime.datetime.now()

  def _WarnForQueueOverflow(self, priority, job_count):
    """Emits a warning about the current queue overflow (for priority class
    @p priority). Only send an email once every _OVERFLOW_WARNING_DELAY second.
    """

    if self._overflow_warning and self._CanWarnForQueueOverflow(priority):
      logger.critical("Queue overflow for priority class '%s'\n" \
        "%d jobs waiting in the %s queue" % (priority, job_count, priority))
      self._last_overflow_warning[priority] = datetime.datetime.now()

  def _GetCurrentQueueDelays(self, job_counts):
    """Returns the current queue delay for each priority class, and sends a
    warning on queue overflow. The queue delay is computed so as to limit the
    processing delay to self._MAX_QUEUE_DELAY."""

    delays = {}
    for queue in job_counts:
      try:
        job_count = job_counts[queue]
        normal_delay = self._delays[queue]
      except KeyError:
        raise PermanentError("Priority queue '%s' not supported." % queue)

      total_processing_time = job_count * normal_delay
      if total_processing_time > self._MAX_QUEUE_DELAY:
        delays[queue] = max(self._MAX_QUEUE_DELAY / job_count, self._min_delay)
      else:
        delays[queue] = normal_delay

      if delays[queue] * job_count > self._MAX_QUEUE_DELAY:
        self._WarnForQueueOverflow(queue, job_count)
    return delays

  def _CanProcessFromQueue(self, queue, queue_delay):
    """Returns True iff the current delay contraints allows to process an
    element from the queue."""

    try:
      if not self._last_jobs[queue]:
        return True
      next_job = self._last_jobs[queue] + datetime.timedelta(0, queue_delay)
      return next_job <= datetime.datetime.now()
    except KeyError:
      raise PermanentError("Priority queue '%s' not supported." % queue)

  def _GetNextPriorityQueue(self, job_counts):
    """Returns the name of the next queue to process an element of. This is an
    iterator."""

    delays = self._GetCurrentQueueDelays(job_counts)
    for queue in self._PRIORITY_ORDER:
      if queue in job_counts and job_counts[queue] > 0:
        while self._CanProcessFromQueue(queue, delays[queue]):
          self._last_jobs[queue] = datetime.datetime.now()
          yield queue

  # Queue processing helpers.
  def _GetJobCounts(self):
    """Returns the number of runnable jobs in each priority queue."""

    sql_query = "SELECT p_priority, COUNT(q_id) AS count FROM gapps_queue " \
      "WHERE %s GROUP BY p_priority" % (self._ACTIVE_JOBS_WHERE_CLAUSE,)
    results = self._sql.Query(sql_query)
    return dict([(row["p_priority"], row["count"]) for row in results])

  def _GetJobFromQueue(self, queue):
    """Fetches a job from the given @p priority queue, and returns the
    corresponding job object (or None if no job was found). Also updates
    the status field of the job."""

    sql_query = "SELECT %s FROM gapps_queue WHERE %s AND p_priority = %%s " \
      "ORDER BY q_id LIMIT 1" % (self._JOB_SELECT_CLAUSE,
                                 self._ACTIVE_JOBS_WHERE_CLAUSE)
    result = self._sql.Query(sql_query, (queue,))
    if not len(result):
      return None

    try:
      j = job.job_registry.Instantiate(result[0]["j_type"],
                                       self._config, self._sql, result[0])
      j.MarkActive()
    except job.JobError, message:
      j = None
      job.Job.MarkFailed(self._sql, result[0]["q_id"],
                         "Job instantiation error: %s" % (message,))
      logger.info("Failed to instantiate job %d: %s" % \
        (result[0]["q_id"], message))
    return j

  def _ProcessJob(self, j):
    """Processes the job (ie. runs it), and handles the errors.
    Note: when job returns properly, we test that either the new status is
    definitive (success ou hardfail), it was an admin request (status is back
    to idle), or the r_softfail_count has been updated.
    If not, we manually set the status to success (default).
    """

    if self._config.get_int('gappsd.read-only') and j.HasSideEffects():
      j.Update(j.STATUS_HARDFAIL, "GAppsd in read-only mode.")
      logger.info("Cancelled <%s>: gappsd in read-only mode." % j.__str__())
      return

    try:
      logger.info("Starting to process <%s>" % (j.__str__(),))
      old_status = j.status()
      j.Run()
      new_status = j.status()

      if new_status[0] not in [j.STATUS_SUCCESS,
                               j.STATUS_HARDFAIL,
                               j.STATUS_IDLE]:
        if new_status[1] == old_status[1]:
          j.Update(j.STATUS_SUCCESS)
      logger.info("Processed <%s>: %s" % (j.__str__(), new_status[0]))
    except (TransientError, database.SQLTransientError), message:
      self._AddTransientError(j, message)
      j.Update(j.STATUS_SOFTFAIL, message)
      logger.info("Processed <%s>: softfail (%s)" % (j.__str__(), message))
    except (PermanentError, database.SQLPermanentError), message:
      j.Update(j.STATUS_HARDFAIL, message)
      logger.info("Processed <%s>: hardfail (%s)" % (j.__str__(), message))

  def _ProcessNextJob(self):
    """Determines the next job to process, and process it."""

    job_counts = self._GetJobCounts()
    for queue in self._GetNextPriorityQueue(job_counts):
      job = self._GetJobFromQueue(queue)
      if job:
        self._ProcessJob(job)
        self._job_counts[queue] += 1

  # Error handling helpers.
  def _AddTransientError(self, j, message):
    """Adds a transient error to the queue's transient error list."""

    self._transient_errors.append({
      "date": datetime.datetime.now(),
      "job": j.__str__(),
      "exc_type": sys.exc_info()[0],
      "message": message,
    })

  def _CheckTransientErrors(self):
    """Handles TransientError exceptions. While permanent errors are bad but
    constant over the time, transient errors can be due to repeated error
    conditions (such as credential failures) that can only be solved by manual
    intervention of administrators.

    Whenever the CredentialError count reaches a threshold, raises a
    Credential (which is supposed to lead to program reconfiguration).
    Whenever the TransientError count reaches a threshold, raises a
    TransientError (which is supposed to lead to Queue restart, and to
    manual intervention after several queue restart)."""

    validity_date = datetime.datetime.now() - \
      datetime.timedelta(0, self._TRANSIENT_ERRORS_VALIDITY)
    while self._transient_errors and \
          self._transient_errors[0]["date"] < validity_date:
      self._transient_errors.pop(0)

    transient_errors = []
    credential_errors = []
    for error in self._transient_errors:
      if issubclass(error["exc_type"], logger.CredentialError):
        credential_errors.append(error)
      else:
        transient_errors.append(error)

    if len(credential_errors) >= self._CREDENTIAL_ERRORS_THRESHOLD:
      logger.critical("Credential errors count above threshold\nError list:\n" +
               pprint.pformat(credential_errors))
      raise logger.CredentialError
    if len(transient_errors) >= self._TRANSIENT_ERRORS_THRESHOLD:
      logger.critical("Transient errors count above threshold\nError list:\n" +
               pprint.pformat(transient_errors))
      raise logger.TransientError

  # Queue runner.
  def _LogStatistics(self):
    """Logs the current statistics, and reset the counters."""

    job_stats = ["%s=%d" % (q, c) for (q, c) in list(self._job_counts.items())]
    logger.info("Queue stats - jobs handled: " + ", ".join(job_stats))
    logger.info("Queue stats - transient errors: " + \
      str(len(self._transient_errors)))
    for queue in self._job_counts:
      self._job_counts[queue] = 0

  def Run(self):
    """Handles the job queue: it retrieves the jobs, run them, and update their
    status."""

    assert(self._min_delay >= 1)
    last_stats = datetime.datetime.now()
    delta_stats = datetime.timedelta(0, self._STATISTICS_DELAY)
    while True:
      self._CheckTransientErrors()
      if datetime.datetime.now() - last_stats > delta_stats:
        self._LogStatistics()
        last_stats = datetime.datetime.now()
      self._ProcessNextJob()
      self._sql.Close()

      time.sleep(self._min_delay)
