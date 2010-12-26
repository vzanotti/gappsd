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

"""Tests for the provisioning features of the GAppsd tools."""

import gappsd.job as job
import gappsd.logger as logger
import gappsd.provisioning as provisioning
import gdata.apps.service
import gdata.service
import testing.config
import mox, unittest

class TestUserJob(unittest.TestCase):
  _JOB_DATA = {
    "q_id": 42, "p_status": "active", "p_entry_date": 1200043549,
    "p_start_date": 1200043559, "j_type": "r_activity", "j_parameters": "{}",
    "r_softfail_count": 0, "r_softfail_date": 1200043259,
  }

  def setUp(self):
    self.config = testing.config.MockConfig()

  def testCheckParametersUsername(self):
    self._JOB_DATA['j_parameters'] = '{"username": "abcd"}'
    provisioning.UserJob(self.config, None, self._JOB_DATA)

    self._JOB_DATA['j_parameters'] = '{"username": "#"}'
    self.assertRaises(job.JobContentError, provisioning.UserJob,
                      self.config, None, self._JOB_DATA)

    self._JOB_DATA['j_parameters'] = '{}'
    self.assertRaises(job.JobContentError, provisioning.UserJob,
                      self.config, None, self._JOB_DATA)

  def testCheckParametersFirstName(self):
    self._JOB_DATA['j_parameters'] = \
      '{"first_name": "beno\uC3AFt", "username": "foo.bar"}'
    provisioning.UserJob(self.config, None, self._JOB_DATA)

    self._JOB_DATA['j_parameters'] = \
      '{"first_name": "#", "username": "foo.bar"}'
    self.assertRaises(job.JobContentError, provisioning.UserJob,
                      self.config, None, self._JOB_DATA)

  def testCheckParametersLastName(self):
    self._JOB_DATA['j_parameters'] = \
      '{"last_name": "beno\uC3AFt", "username": "foo.bar"}'
    provisioning.UserJob(self.config, None, self._JOB_DATA)

    self._JOB_DATA['j_parameters'] = \
      '{"last_name": "#", "username": "foo.bar"}'
    self.assertRaises(job.JobContentError, provisioning.UserJob,
                      self.config, None, self._JOB_DATA)

class TestUserCreateJob(mox.MoxTestBase):
  _JOB_DATA = {
    "q_id": 42, "p_status": "active", "p_entry_date": 1200043549,
    "p_start_date": 1200043559, "j_type": "r_activity",
    "r_softfail_count": 0, "r_softfail_date": 1200043259,
    "j_parameters": '{"username":"foo.bar","first_name":"foo","last_name":"bar","password":"0123456789abcdef0123456789abcdef01234567"}',
  }

  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.client = self.mox.CreateMock(provisioning.ProvisioningApiClient)
    self.config = testing.config.MockConfig()
    provisioning.provisioning_api_client = self.client

  def testCreateExistingAccount(self):
    self.client.TryRetrieveUser("foo.bar").AndReturn(True)
    self.mox.ReplayAll()

    j = provisioning.UserCreateJob(self.config, None, self._JOB_DATA)
    self.assertRaises(logger.PermanentError, j.Run)

  def testCreateAccount(self):
    # TODO
    pass

class TestUserDeleteJob(mox.MoxTestBase):
  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.client = self.mox.CreateMock(provisioning.ProvisioningApiClient)
    self.config = testing.config.MockConfig()
    provisioning.provisioning_api_client = self.client

  # TODO

class TestUserSynchronizeJob(mox.MoxTestBase):
  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.client = self.mox.CreateMock(provisioning.ProvisioningApiClient)
    self.config = testing.config.MockConfig()
    provisioning.provisioning_api_client = self.client

  # TODO

class TestUserUpdateJob(mox.MoxTestBase):
  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.client = self.mox.CreateMock(provisioning.ProvisioningApiClient)
    self.config = testing.config.MockConfig()
    provisioning.provisioning_api_client = self.client

  # TODO

class TestProvisioningApiClient(mox.MoxTestBase):
  def setUp(self):
    mox.MoxTestBase.setUp(self)
    self.service = self.mox.CreateMock(gdata.apps.service.AppsService)
    self.client = provisioning.ProvisioningApiClient(
      testing.config.MockConfig(),
      self.service)

  def testRenewToken(self):
    self.service.captcha_url = 'http://example.com'
    self.service.ProgrammaticLogin()
    self.service.ProgrammaticLogin().AndRaise(gdata.service.BadAuthentication)
    self.service.ProgrammaticLogin().AndRaise(gdata.service.CaptchaRequired)
    self.service.ProgrammaticLogin().AndRaise(gdata.service.Error)
    self.service.ProgrammaticLogin().AndRaise(Exception)
    self.mox.ReplayAll()

    self.client._RenewToken()
    self.assertRaises(logger.CredentialError, self.client._RenewToken)
    self.assertRaises(logger.CredentialError, self.client._RenewToken)
    self.assertRaises(logger.TransientError, self.client._RenewToken)
    self.assertRaises(logger.TransientError, self.client._RenewToken)

  def testUpdateUser(self):
    kException42 = gdata.apps.service.AppsForYourDomainException(
      {"status": "42"})
    kException42.error_code = gdata.apps.service.UNKOWN_ERROR
    kException200 = gdata.apps.service.AppsForYourDomainException(
      {"status": "200"})
    kException200.error_code = gdata.apps.service.ENTITY_DOES_NOT_EXIST
    kException200.reason = ""

    self.service.GetClientLoginToken().AndReturn(True)
    self.service.UpdateUser("foo.bar", None).AndReturn(True)
    self.service.GetClientLoginToken().AndReturn(True)
    self.service.UpdateUser("foo.bar", None).AndRaise(kException42)
    self.service.GetClientLoginToken().AndReturn(True)
    self.service.UpdateUser("foo.bar", None).AndRaise(kException200)
    self.service.GetClientLoginToken().AndReturn(True)
    self.service.UpdateUser("foo.bar", None).AndRaise(Exception)
    self.mox.ReplayAll()

    self.assertEquals(True, self.client.UpdateUser("foo.bar", None))
    self.assertRaises(logger.TransientError, self.client.UpdateUser,
                      "foo.bar", None)
    self.assertRaises(logger.PermanentError, self.client.UpdateUser,
                      "foo.bar", None)
    self.assertRaises(Exception, self.client.UpdateUser, "foo.bar", None)

  def testTryUpdateUser(self):
    kExceptionEntity = gdata.apps.service.AppsForYourDomainException(
      {"status": "42"})
    kExceptionEntity.error_code = gdata.apps.service.ENTITY_DOES_NOT_EXIST
    kExceptionUser = gdata.apps.service.AppsForYourDomainException(
      {"status": "200"})
    kExceptionUser.error_code = gdata.apps.service.USER_SUSPENDED
    kExceptionUser.reason = ""

    self.service.GetClientLoginToken().AndReturn(True)
    self.service.UpdateUser("foo.bar", None).AndReturn(True)
    self.service.GetClientLoginToken().AndReturn(True)
    self.service.UpdateUser("foo.bar", None).AndRaise(kExceptionEntity)
    self.service.GetClientLoginToken().AndReturn(True)
    self.service.UpdateUser("foo.bar", None).AndRaise(kExceptionUser)
    self.mox.ReplayAll()

    self.assertEquals(True, self.client.TryUpdateUser("foo.bar", None))
    self.assertEquals(None, self.client.TryUpdateUser("foo.bar", None))
    self.assertRaises(logger.PermanentError, self.client.TryUpdateUser,
                      "foo.bar", None)
