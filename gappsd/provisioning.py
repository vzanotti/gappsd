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

"""Implements User accounts manipulation jobs, using the Provisioning API
provided by Google."""

import gdata.service
import gdata.apps.service
import re
import traceback

import account, job, queue
from . import logger
from .logger import PermanentError, TransientError

class UserJob(job.Job):
  """Base class for User account manipulation jobs. It provides basic parameter
  checking as well as initialization."""

  _FIELDS_REGEXP = {
    "username": re.compile(r"^[a-z0-9._-]+", re.I),
    "first_name": re.compile(r"^[\w /.'-]{1,40}$", re.I | re.UNICODE),
    "last_name": re.compile(r"^[\w /.'-]{1,40}$", re.I | re.UNICODE),
    "password": re.compile(r"^[a-f0-9]{40}$", re.I),
    "suspended": re.compile(r"^(true|false)$", re.I),
  }

  def __init__(self, config, sql, job_dict):
    job.Job.__init__(self, config, sql, job_dict)
    self._api_client = GetProvisioningApiClientInstance(config)
    self._CheckParameters()

  def __str__(self):
    job_string = job.Job.__str__(self)
    if "username" in self._parameters:
      return job_string + ", user '%s'" % self._parameters["username"]
    else:
      return job_string + ", user unknown"

  def _CheckParameters(self):
    """Checks that the JSON-encoded parameters of the job are valid."""

    if not "username" in self._parameters:
      raise job.JobContentError("Field 'username' missing.")
    for field in self._FIELDS_REGEXP:
      if field in self._parameters:
        if not isinstance(self._parameters[field], basestring):
          self._parameters[field] = unicode(self._parameters[field])
        if not self._FIELDS_REGEXP[field].match(self._parameters[field]):
          raise job.JobContentError("Field '%s' did not match regexp '%s'." % \
            (field, self._FIELDS_REGEXP[field]))


class UserCreateJob(UserJob):
  """Implements the account creation request."""

  _MANDATORY_FIELDS = ["username", "first_name", "last_name", "password"]

  def __init__(self, config, sql, job_dict):
    UserJob.__init__(self, config, sql, job_dict)

  def _CheckParameters(self):
    """Checks that the JSON-encoded parameters of the job are valid."""

    UserJob._CheckParameters(self)
    for field in self._MANDATORY_FIELDS:
      if field not in self._parameters:
        raise job.JobContentError("Field '%s' missing." % field)

  def Run(self):
    """Creates a new Google account (if the @p username did not exist), and
    updates the SQL database."""

    # Checks that no account exists with this name.
    user_entry = self._api_client.TryRetrieveUser(self._parameters["username"])
    if user_entry:
      raise PermanentError("An account for user '%s' already exists." % \
        self._parameters["username"])

    # Creates the account on Google side.
    if "suspended" in self._parameters:
      suspended = self._parameters["suspended"].lower()
    else:
      suspended = 'false'

    user = self._api_client.CreateUser(
      user_name = self._parameters["username"],
      family_name = self._parameters["last_name"],
      given_name = self._parameters["first_name"],
      password = self._parameters["password"],
      password_hash_function = "SHA-1",
      suspended = suspended)

    # Creates the account in the SQL database.
    a = account.LoadAccountFromDatabase(self._sql, self._parameters["username"])
    UserSynchronizeJob.Synchronize(self._sql, account=a, user_entry=user)

    self.Update(self.STATUS_SUCCESS)


class UserDeleteJob(UserJob):
  """Implements the account deletion request, with security checks -- the job
  can only be executed in admin mode."""

  def __init__(self, config, sql, job_dict):
    UserJob.__init__(self, config, sql, job_dict)

  def Run(self):
    """Processes the account deletetion, with security checks: only
    administrators can delete accounts, and only normal accounts can be
    deleted."""

    # Refuses to delete accounts in non-admin mode.
    if not self._config.get_int("gappsd.admin-only-jobs"):
      self.MarkAdmin()
      return

    # Checks that the user entry actually exists.
    user = self._api_client.TryRetrieveUser(self._parameters["username"])
    if not user:
      raise PermanentError("User '%s' did not exist. Deletion failed." % \
        self._parameters["username"])

    # Checks that the job is not requesting the deletion of an administrator.
    if user.login.admin != 'false':
      raise PermanentError("Administrators cannot be deleted directly, you" \
        " must remove their admin status first.")

    # Removes the account from the databases.
    self._api_client.DeleteUser(self._parameters["username"])
    a = account.LoadAccountFromDatabase(self._sql, self._parameters["username"])
    if a:
      a.Delete(self._sql)

    self.Update(self.STATUS_SUCCESS)


class UserSynchronizeJob(UserJob):
  """Implements the account synchronization job; such a job aims at
  re-synchronizing the SQL database with the Google account database. Also
  implements static synchronization methods."""

  def __init__(self, config, sql, job_dict):
    UserJob.__init__(self, config, sql, job_dict)

  # Synchronization methods.
  @staticmethod
  def Synchronize(sql, account, user_entry):
    """Synchronizes the SQL database with the Google account database for
    the user given as argument. The synchronization is only one-way (ie. the
    information from the Google account are updated in the SQL database)."""

    if not account and not user_entry:
      pass
    elif not account:
      UserSynchronizeJob.SynchronizeNoSQL(sql, user_entry)
    elif not user_entry:
      UserSynchronizeJob.SynchronizeNoGoogle(sql, account)
    else:
      UserSynchronizeJob.SynchronizeGoogleToSQL(sql, account, user_entry)

  @staticmethod
  def SynchronizeNoSQL(sql, user_entry):
    """Creates the SQL account based on the Google's UserEntry data."""

    admin = user_entry.login.admin == 'true'
    suspended = user_entry.login.suspended == 'true'

    a = account.Account(user_entry.login.user_name)
    a.set('g_first_name', user_entry.name.given_name)
    a.set('g_last_name', user_entry.name.family_name)
    a.set('g_status', a.STATUS_DISABLED if suspended else a.STATUS_ACTIVE)
    a.set('g_admin', admin)
    a.Create(sql)

  @staticmethod
  def SynchronizeNoGoogle(sql, account):
    """Re-initializes the SQL account to the 'unprovisioned' state."""

    account.set('g_account_id', None)
    account.set('g_status', account.STATUS_UNPROVISIONED)
    account.set('g_admin', None)
    account.set('g_suspension', None)
    account.set('r_disk_usage', None)
    account.set('r_creation', None)
    account.set('r_last_login', None)
    account.set('r_last_webmail', None)
    account.Update(sql)

  @staticmethod
  def SynchronizeGoogleToSQL(sql, account, user_entry):
    """Synchronizes the SQL account with the information contained in the
    Google's UserEntry object."""

    if account.get("g_account_name") != user_entry.login.user_name:
      raise PermanentError( \
        "Cannot synchronize accounts with different usernanames (%s - %s)" & \
        (account.get("g_account_name"), user_entry.login.user_name))

    # Updates silently non-critical fields.
    account.set('g_first_name', user_entry.name.given_name)
    account.set('g_last_name', user_entry.name.family_name)

    # Updates verbosely critical fields.
    account_suspended = account.get('g_status') or account.STATUS_UNPROVISIONED
    suspended = user_entry.login.suspended.lower() == 'true'
    if suspended and account_suspended != account.STATUS_DISABLED:
      logger.error(
        "Account '%s' is now suspended\nreason = '%s'" % \
        (account.get("g_account_name"), account.get("g_suspension")))

    account_admin = account.get('g_admin') or False
    admin = user_entry.login.admin == 'true'
    if admin and not account_admin:
      logger.error(
        "Account '%s' is now administrator of the domain" % \
        account.get("g_account_name"))
    elif not admin and account_admin:
      logger.error(
        "Account '%s' is not anymore administrator of the domain" % \
        account.get("g_account_name"))

    account.set('g_admin', admin)
    account.set('g_status',
                account.STATUS_DISABLED if suspended else account.STATUS_ACTIVE)

    account.Update(sql)

  # Job execution.
  def Run(self):
    """Loads the two version of the account (SQL and Google), and
    synchronizes them."""

    a = account.LoadAccountFromDatabase(self._sql, self._parameters["username"])
    user = self._api_client.TryRetrieveUser(self._parameters["username"])
    UserSynchronizeJob.Synchronize(self._sql, a, user)

    self.Update(self.STATUS_SUCCESS)


class UserUpdateJob(UserJob):
  """Implements the user update job, with security checks: only administrators
  can change admin mode, or change password/suspension for other administrators.
  """

  def __init__(self, config, sql, job_dict):
    UserJob.__init__(self, config, sql, job_dict)

  def Run(self):
    # Retrieves the UserEntry and checks the existence of the user.
    user = self._api_client.TryRetrieveUser(self._parameters["username"])
    if not user:
      raise PermanentError( \
        "User '%s' do not exist, cannot update its account." % \
        self._parameters["username"])

    # In non-privileged mode, refuses to update the password, suspension status,
    # or admin status of an administrator.
    if not self._config.get_int("gappsd.admin-only-jobs"):
      if "admin" in self._parameters or (user.login.admin == 'true' and \
          ("suspended" in self._parameters or "password" in self._parameters)):
        self.MarkAdmin()
        return

    # Updates the Google account.
    if "admin" in self._parameters:
      user.login.admin = self._parameters["admin"]
    if "first_name" in self._parameters:
      user.name.given_name = self._parameters["first_name"]
    if "last_name" in self._parameters:
      user.name.family_name = self._parameters["last_name"]
    if "password" in self._parameters:
      user.login.password = self._parameters["password"]
      user.login.hash_function_name = "SHA-1"
    if "suspended" in self._parameters:
      user.login.suspended = self._parameters["suspended"].lower()

    user = self._api_client.UpdateUser(self._parameters["username"], user)

    # Updates the SQL account.
    a = account.LoadAccountFromDatabase(self._sql, self._parameters["username"])
    UserSynchronizeJob.Synchronize(self._sql, account=a, user_entry=user)

    self.Update(self.STATUS_SUCCESS)


class ProvisioningApiClient(object):
  """Proxy layer between the gappsd framework and the Google Apps Provisioning
  API client. Handles token management (token request, error handling, ...)
  and 'safe' API requests (by only throwing gappsd exceptions).

  Example usage:
    provisioning = ProvisioningApiClient(config)
    provisioning.CreateUser(Cf. google/gdata/apps/service.py for usage)
  """

  def __init__(self, config, apps_service=None):
    self._domain = config.get_string("gapps.domain")
    self._admin_password = config.get_string("gapps.admin-api-password")
    self._admin_email = \
      "%s@%s" % (config.get_string("gapps.admin-api-username"), self._domain)

    self._service = apps_service
    if not self._service:
      self._RenewService()

  def _RenewService(self):
    """Renew the local AppsService object."""

    del self._service
    self._service = gdata.apps.service.AppsService( \
      email=self._admin_email,
      domain=self._domain,
      password=self._admin_password)


  def _RenewToken(self):
    """Renews (or requests) a token, by instantiating a new AppsService object.
    Properly handles raised exception by re-raising the appropriate gappsd
    exceptions."""

    try:
      logger.info("Provisioning API - Requesting authentication token")
      self._service.ProgrammaticLogin()
      logger.info("Provisioning API - Authentication succedeed")
    except gdata.service.BadAuthentication, error:
      logger.critical("Provisioning API - Authentication refused")
      raise logger.CredentialError("Bad credential for Provisioning API")
    except gdata.service.CaptchaRequired, error:
      logger.critical( \
        "Provisioning API - Captcha required for authentication\n" + \
        "Please visit:\n  %s\n" % self._service.captcha_url + \
        "and use %s's identity to solve the captcha.\n" % self._admin_email + \
        "\nDo not forget to restart gappsd !")
      raise logger.CredentialError("Captcha required for Provisioning API")
    except gdata.service.Error, error:
      logger.info("Provisioning API - Authentication failed with 403 error")
      raise TransientError( \
        "403 error while authenticating for Provisioning API")
    except Exception, error:
      logger.info("Provisioning API - Authentication failed with unknown error")
      raise TransientError( \
        "Other error for Provisioning API authentication:\n%s" % error)

  def LogOut(self):
    """Invalidates the current token, by calling the logout method on
    Google-side. Should be called whenever the token will not be used in the
    future."""

    # TODO(vzanotti): implement.
    pass

  def _ProcessApiRequest(self, method, pargs, nargs, acceptable_error_codes=None):
    """Common API Request processor: calls a function of the underlying service,
    and process its errors. @p method is a method of the self._service object,
    and @p args is the dictionary of its parameters.

    If the request fails, and the error code is in the acceptable_error_codes @p
    then it returns None (instead of raising the appropriate exception).
    """

    if not self._service.auth_token:
      self._RenewToken()
    if not acceptable_error_codes:
      acceptable_error_codes = ()

    try:
      result = method(*pargs, **nargs)
    except gdata.apps.service.AppsForYourDomainException, error:
      if error.error_code == gdata.apps.service.UNKOWN_ERROR:
        # If HTTP status is 401, it probably means the token is invalid
        # or expired.
        if error.args[0]["status"] == 401:
          logger.info("Provisioning API - got 401 http error code")
          self._RenewService()
          raise TransientError("Provisioning API - Invalid token")
        elif error.args[0]["status"] == 500:
          logger.info("Provisioning API - Internal Server Error")
          raise TransientError("Provisioning API - Internal Server Error")
        else:
          logger.info("Provisioning API - Unknown error: %s" % error)
          raise TransientError("Provisioning API - Unknown error: %s" % error)

      # Other errors are permanent and related to the request itself.
      if not error.error_code in acceptable_error_codes:
        logger.info("Provisioning API - Permanent error %d" % error.error_code)
        raise PermanentError("Provisioning API - Error %d (%s)" % \
          (error.error_code, error.reason))
      else:
        result = None
    except gdata.service.RequestError, error:
      logger.info("Provisioning API - Request error %d" % \
        error.args[0]["status"])
      raise TransientError("Provisioning API - Request error %d (%s)" % \
        (error.args[0]["status"], error.args[0]["body"]))
    except Exception, error:
      logger.info("Provisioning API - Request failed with unknown error")
      raise TransientError("Other error for Provisioning API request:\n" + \
        traceback.format_exc(error))

    # Temporary workaround for incorrect UTF-8 processing in gdata-python-client.
    # Cf. http://groups.google.com/group/google-apps-apis/browse_thread/thread/dfc460bb4ad387fb/74278fcf03db27f8?hl=en#74278fcf03db27f8
    # Cf. http://code.google.com/p/gdata-python-client/issues/detail?id=101
    if isinstance(result, gdata.apps.UserEntry):
      result.name.family_name = result.name.family_name.decode("utf8")
      result.name.given_name = result.name.given_name.decode("utf8")

    return result

  # Proxy methods, used to normalize the raised exceptions. Methods starting
  # with "Try" won't raise exceptions on ENTITY_DOES_NOT_EXIST errors.
  def CreateUser(self, *pargs, **nargs):
    return self._ProcessApiRequest(self._service.CreateUser, pargs, nargs)

  def RetrieveUser(self, *pargs, **nargs):
    return self._ProcessApiRequest(self._service.RetrieveUser, pargs, nargs)

  def TryRetrieveUser(self, *pargs, **nargs):
    return self._ProcessApiRequest(
      self._service.RetrieveUser, pargs, nargs,
      (gdata.apps.service.ENTITY_DOES_NOT_EXIST,))

  def UpdateUser(self, *pargs, **nargs):
    return self._ProcessApiRequest(self._service.UpdateUser, pargs, nargs)

  def TryUpdateUser(self, *pargs, **nargs):
    return self._ProcessApiRequest(
      self._service.UpdateUser, pargs, nargs,
      (gdata.apps.service.ENTITY_DOES_NOT_EXIST,))

  def DeleteUser(self, *pargs, **nargs):
    return self._ProcessApiRequest(self._service.DeleteUser, pargs, nargs)

  def TryDeleteUser(self, *pargs, **nargs):
    return self._ProcessApiRequest(
      self._service.DeleteUser, pargs, nargs,
      (gdata.apps.service.ENTITY_DOES_NOT_EXIST,))

def GetProvisioningApiClientInstance(config=None):
  """Returns the global provisioning APi client instance (instantiates it if
  needed). Returns None if there is no current client and config is None."""
  global provisioning_api_client
  if provisioning_api_client is None:
    if not config:
      return None
    provisioning_api_client = ProvisioningApiClient(config)
  return provisioning_api_client

def LogOut():
  """Eventually invalidates the provisioning tokens -- when available.
  Should be called at the end of each session to ensure token safety."""
  
  client = GetProvisioningApiClientInstance()
  if client:
    client.LogOut()

# Module initialization.
provisioning_api_client = None
job.job_registry.Register('u_create', UserCreateJob)
job.job_registry.Register('u_delete', UserDeleteJob)
job.job_registry.Register('u_sync', UserSynchronizeJob)
job.job_registry.Register('u_update', UserUpdateJob)
