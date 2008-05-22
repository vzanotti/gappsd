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

"""TODO"""

import gappsd.job as job
import gappsd.logger as logger
import gappsd.provisioning as provisioning
import gdata.apps.service
import gdata.service
import testing.config
import unittest

class MockGDataService(object):
  """Mocks gdata.apps.service.AppsService, so as to test the
  provisioning.ProvisioningApiClient class."""

  def __init__(self):
    self.auth_token = None
    self.captcha_url = ""
    self.programmatic_login_answer = None

    self.create_user_answer = None
    self.create_user_parameters = ()
    self.retrieve_user_answer = None
    self.retrieve_user_parameters = ()
    self.update_user_answer = None
    self.update_user_parameters = ()
    self.delete_user_answer = None
    self.delete_user_parameters = ()

  def ProgrammaticLogin(self):
    if isinstance(self.programmatic_login_answer, Exception):
      raise self.programmatic_login_answer
    return self.programmatic_login_answer

  def CreateUser(self, user_name, family_name, given_name,
                 password, suspended='false', password_hash_function=None):
    self.create_user_parameters = (user_name, family_name, given_name,
      password, suspended, password_hash_function)
    if isinstance(self.create_user_answer, Exception):
      raise self.create_user_answer
    return self.create_user_answer

  def RetrieveUser(self, user_name):
    self.retrieve_user_parameters = user_name
    if isinstance(self.retrieve_user_answer, Exception):
      raise self.retrieve_user_answer
    return self.retrieve_user_answer

  def UpdateUser(self, user_name, user_entry):
    self.update_user_parameters = (user_name, user_entry)
    if isinstance(self.update_user_answer, Exception):
      raise self.update_user_answer
    return self.update_user_answer

  def DeleteUser(self, user_name):
    self.delete_user_parameters = user_name
    if isinstance(self.delete_user_answer, Exception):
      raise self.delete_user_answer
    return self.delete_user_answer

class MockProvisioningApiClient(provisioning.ProvisioningApiClient):
  def __init__(self):
    self.service = MockGDataService()
    provisioning.ProvisioningApiClient(
      testing.config.MockConfig(),
      self.service)

  @staticmethod
  def PrepareGlobalMock():
    global provisioning_api_client
    provisioning_api_client = MockProvisioningApiClient()


class TestUserJob(unittest.TestCase):
  _JOB_DATA = {
    "q_id": 42, "p_status": "active", "p_entry_date": 1200043549,
    "p_start_date": 1200043559, "j_type": "r_activity", "j_parameters": "{}",
    "r_softfail_count": 0, "r_softfail_date": 1200043259,
  }

  def setUp(self):
    MockProvisioningApiClient.PrepareGlobalMock()
    self.config = testing.config.MockConfig()
    self.sql = testing.database.MockSQL()

  def testCheckParametersUsername(self):
    self._JOB_DATA['j_parameters'] = '{"username": "abcd"}'
    provisioning.UserJob(self.config, self.sql, self._JOB_DATA)

    self._JOB_DATA['j_parameters'] = '{"username": "#"}'
    self.assertRaises(job.JobContentError, provisioning.UserJob,
                      self.config, self.sql, self._JOB_DATA)

    self._JOB_DATA['j_parameters'] = '{}'
    self.assertRaises(job.JobContentError, provisioning.UserJob,
                      self.config, self.sql, self._JOB_DATA)

  def testCheckParametersFirstName(self):
    self._JOB_DATA['j_parameters'] = \
      '{"first_name": "beno\uC3AFt", "username": "foo.bar"}'
    provisioning.UserJob(self.config, self.sql, self._JOB_DATA)

    self._JOB_DATA['j_parameters'] = \
      '{"first_name": "#", "username": "foo.bar"}'
    self.assertRaises(job.JobContentError, provisioning.UserJob,
                      self.config, self.sql, self._JOB_DATA)

  def testCheckParametersLastName(self):
    self._JOB_DATA['j_parameters'] = \
      '{"last_name": "beno\uC3AFt", "username": "foo.bar"}'
    provisioning.UserJob(self.config, self.sql, self._JOB_DATA)

    self._JOB_DATA['j_parameters'] = \
      '{"last_name": "#", "username": "foo.bar"}'
    self.assertRaises(job.JobContentError, provisioning.UserJob,
                      self.config, self.sql, self._JOB_DATA)

class TestUserCreateJob(unittest.TestCase):
  _JOB_DATA = {
    "q_id": 42, "p_status": "active", "p_entry_date": 1200043549,
    "p_start_date": 1200043559, "j_type": "r_activity",
    "r_softfail_count": 0, "r_softfail_date": 1200043259,
    "j_parameters": '{"username":"foo.bar","first_name":"foo","last_name":"bar","password":"0123456789abcdef0123456789abcdef01234567"}',
  }

  def setUp(self):
    self.config = testing.config.MockConfig()
    self.service = MockGDataService()
    self.sql = testing.database.MockSQL()
    provisioning.provisioning_api_client = \
      provisioning.ProvisioningApiClient(self.config, self.service)

  def testCreateExistingAccount(self):
    self.service.retrieve_user_answer = True
    j = provisioning.UserCreateJob(self.config, self.sql, self._JOB_DATA)
    self.assertRaises(logger.PermanentError, j.Run)
    self.assertEquals(self.service.retrieve_user_parameters, "foo.bar")

  def testCreateAccount(self):
    # TODO
    pass

class TestUserDeleteJob(unittest.TestCase):
  def setUp(self):
    self.config = testing.config.MockConfig()
    self.service = MockGDataService()
    self.sql = testing.database.MockSQL()
    provisioning.provisioning_api_client = \
      provisioning.ProvisioningApiClient(self.config, self.service)

  # TODO

class TestUserSynchronizeJob(unittest.TestCase):
  def setUp(self):
    self.config = testing.config.MockConfig()
    self.service = MockGDataService()
    self.sql = testing.database.MockSQL()
    provisioning.provisioning_api_client = \
      provisioning.ProvisioningApiClient(self.config, self.service)

  # TODO

class TestUserUpdateJob(unittest.TestCase):
  def setUp(self):
    self.config = testing.config.MockConfig()
    self.service = MockGDataService()
    self.sql = testing.database.MockSQL()
    provisioning.provisioning_api_client = \
      provisioning.ProvisioningApiClient(self.config, self.service)

  # TODO

class TestProvisioningApiClient(unittest.TestCase):
  def setUp(self):
    self.service = MockGDataService()
    self.client = provisioning.ProvisioningApiClient(
      testing.config.MockConfig(),
      self.service)

  def testRenewToken(self):
    self.service.programmatic_login_answer = None
    self.client._RenewToken()

    self.service.programmatic_login_answer = gdata.service.BadAuthentication()
    self.assertRaises(logger.CredentialError, self.client._RenewToken)

    self.service.programmatic_login_answer = gdata.service.CaptchaRequired()
    self.assertRaises(logger.CredentialError, self.client._RenewToken)

    self.service.programmatic_login_answer = gdata.service.Error()
    self.assertRaises(logger.TransientError, self.client._RenewToken)

    self.service.programmatic_login_answer = Exception()
    self.assertRaises(logger.TransientError, self.client._RenewToken)

  def testUpdateUser(self):
    self.service.update_user_answer = True
    self.assertEquals(True, self.client.UpdateUser("foo.bar", None))

    self.service.update_user_answer = \
      gdata.apps.service.AppsForYourDomainException({"status": "42"})
    self.service.update_user_answer.error_code = gdata.apps.service.UNKOWN_ERROR
    self.assertRaises(logger.TransientError, self.client.UpdateUser,
                      "foo.bar", None)

    self.service.update_user_answer = \
      gdata.apps.service.AppsForYourDomainException({"status": "200"})
    self.service.update_user_answer.error_code = \
      gdata.apps.service.ENTITY_DOES_NOT_EXIST
    self.service.update_user_answer.reason = ""
    self.assertRaises(logger.PermanentError, self.client.UpdateUser,
                      "foo.bar", None)

    self.service.update_user_answer = Exception()
    self.assertRaises(Exception, self.client.UpdateUser, "foo.bar", None)

  def testTryUpdateUser(self):
    self.service.update_user_answer = True
    self.assertEquals(True, self.client.TryUpdateUser("foo.bar", None))

    self.service.update_user_answer = \
      gdata.apps.service.AppsForYourDomainException({"status": "200"})
    self.service.update_user_answer.error_code = \
      gdata.apps.service.ENTITY_DOES_NOT_EXIST
    self.assertEquals(None, self.client.TryUpdateUser("foo.bar", None))

    self.service.update_user_answer = \
      gdata.apps.service.AppsForYourDomainException({"status": "200"})
    self.service.update_user_answer.error_code = \
      gdata.apps.service.USER_SUSPENDED
    self.service.update_user_answer.reason = ""
    self.assertRaises(logger.PermanentError, self.client.TryUpdateUser,
                      "foo.bar", None)
