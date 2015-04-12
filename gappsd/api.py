#!/usr/bin/python
#
# Copyright (C) 2015 Polytechnique.org
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

"""Providers for the various authenticated endpoints of the Google Admin API."""

import httplib2

from . import logger
from .logger import PermanentError, TransientError

from google.apiclient.discovery import build
from google.apiclient.errors import HttpError
from google.oauth2client.client import SignedJwtAssertionCredentials

def _GetApiService(config, service, scope):
  return build('admin', service, credentials=_GetCredentials(config, scope))

def _GetCredentials(config, scope):
  return SignedJwtAssertionCredentials(
      service_account_name=config.get_string("gapps.oauth2-client"),
      private_key= open(config.get_string("gapps.oauth2-secret")).read(),
      scope=scope,
      sub=config.get_string("gapps.oauth2-user"))

def GetDirectoryService(config):
  return _GetApiService(
      config, service="directory_v1",
      scope="https://www.googleapis.com/auth/admin.directory.user")

def GetReportsService(config):
  return _GetApiService(
      config, service="reports_v1",
      scope="https://www.googleapis.com/auth/admin.reports.usage.readonly")

def HandleError(error):
  if isinstance(error, httplib2.HttpLib2Error):
    logger.info("HTTP Error: %s", error)
    raise TransientError(error)
  elif isinstance(error, HttpError):
    if error.resp.status == 500:
      logger.info("Internal API Error: %s", error)
      raise TransientError(error)
    else:
      logger.info("API Error: %s", error)
      raise PermanentError(error)
  else:
    logger.info("Unknown error: %s", error)
    raise TransientError(error)

def HandleErrorAllowMissing(error):
  if isinstance(error, HttpError) and error.resp.status == 404:
    return None
  HandleError(error)
