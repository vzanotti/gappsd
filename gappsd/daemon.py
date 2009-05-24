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

"""Implementation of the GApps daemon runner."""

import datetime
import os
import pprint
import sys
import time
import traceback

import config, database, queue
import job, provisioning, reporting
from . import logger
from .logger import CredentialError, TransientError

class Daemon(object):
  """The GApps daemon runner: initializes the database, configuration and
  logging helpers, runs the job Queue, and handles its errors (by switching
  to a "backup-doing-nothing-but-sending-emails" mode in case of problems."""

  _BACKUP_EMAIL_INTERVAL = 3600
  _TRANSIENT_ERROR_RESTART_DELAY = 20
  _TRANSIENT_ERRORS_VALIDITY = 3600
  _TRANSIENT_ERRORS_THRESHOLD = 4
  _SAFETY_RESTART_DELAY = 10

  def __init__(self, config_file, log_to_stderr=False):
    self._config = config.Config(config_file)
    logger.InitializeLogging(self._config, log_to_stderr)
    self._sql = database.SQL(self._config)
    self._transient_errors = []

    max_run_time = self._config.get_int("gappsd.max-run-time")
    self._deadline = datetime.datetime.now() + \
        datetime.timedelta(0, max_run_time);

  def _RunInBackupMode(self):
    """Runs the GApps daemon in backup mode: every hour, it sends a reminder
    email to the admin, waiting for a manual restart."""

    while True:
      time.sleep(self._BACKUP_EMAIL_INTERVAL)
      logger.critical(
        "Running in backup mode -- waiting for admin intervention !")

  def _CheckForTransientErrors(self):
    """Returns True iff the number of recent errors is above the threshold."""

    validity_date = datetime.datetime.now() - \
      datetime.timedelta(0, self._TRANSIENT_ERRORS_VALIDITY)
    while self._transient_errors and \
          self._transient_errors[0]["date"] < validity_date:
      self._transient_errors.pop(0)

    return len(self._transient_errors) >= self._TRANSIENT_ERRORS_THRESHOLD

  def _RestartDaemon(self):
    """Restarts the Python daemon, using the execl command. This wipe out the
    current process, and replaces it by a new version."""

    # Wait for a safety interval to avoid execvp flooding.
    time.sleep(self._SAFETY_RESTART_DELAY)

    os.execvp(sys.argv[0], sys.argv)

  def Run(self):
    """Runs the GApps daemon, using the Queue facilities. When a fatal exception
    is catched (ie a Credential, several TransientError, or any other
    unknown exception), it switches to backup mode."""

    while True:
      try:
        q = queue.Queue(self._config, self._sql, self._deadline)
        q.Run()
      except KeyboardInterrupt, error:
        logger.warning("Received keyboard interruption, aborting gracefully...")
        provisioning.LogOut()
        reporting.LogOut()
        sys.exit(0)
      except CredentialError, error:
        logger.critical(
          "Received CredentialError -- switching to backup mode\n" + error)
        self._RunInBackupMode()
      except TransientError, error:
        self._transient_errors.append({"date": datetime.datetime.now(),
                                      "error": error})
        if self._CheckForTransientErrors():
          logger.critical(
            "Received TransientError -- switching to backup mode\n" \
            "List of errors:\n" + pprint.pformat(self._transient_errors))
          self._RunInBackupMode()
        else:
          logger.info("Received TransientError\n" + pprint.pformat(error))
      except Exception, error:
        logger.critical(
          "Received unknown exception -- switching to backup mode\n" + \
          traceback.format_exc(error))
        self._RunInBackupMode()

      # Check that we have not run past the deadline. If so, we just reload the
      # whole program.
      if datetime.datetime.now() > self._deadline:
        logger.warning("Went past the runtime deadline, restarting the daemon")
        provisioning.LogOut()
        reporting.LogOut()
        self._RestartDaemon()

      time.sleep(self._TRANSIENT_ERROR_RESTART_DELAY)
