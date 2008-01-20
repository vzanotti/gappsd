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

"""Configuration file helper for the GAppsd project."""

import ConfigParser

class MissingValueError(Exception):
  """Indicates the absence of a mandatory value in the configuration file."""
  pass

class Config(object):
  """Holds and serves configuration values. Values are initialized from a DOS
  .ini formatted file. Values stored with key "k" under section "s" in the ini
  files will be available as "s.k".

  Example usage:
    config = Config("gappsd.conf")
    config.get_string("gapps.domain")
  """

  def __init__(self, config_file):
    """Initializes the default parameter values, tries to load config from @p
    file, and checks that all options have a non-None value."""
    self._data = {
      'mysql.hostname': None,
      'mysql.username': None,
      'mysql.password': "",
      'mysql.database': None,

      'gapps.domain': None,
      'gapps.admin-api-username': None,
      'gapps.admin-api-password': None,
      'gapps.admin-email': None,

      'gappsd.activity-backlog': 30,
      'gappsd.admin-only-jobs': False,
      'gappsd.job-softfail-delay': 300,
      'gappsd.job-softfail-threshold': 4,
      'gappsd.logfile-backlog': 90,
      'gappsd.logfile-name': '',
      'gappsd.logfile-rotation': 1,
      'gappsd.logmail': False,
      'gappsd.logmail-delay': 1800,
      'gappsd.logmail-domain-in-subject': False,
      'gappsd.logmail-smtp': '',
      'gappsd.queue-min-delay': 2,
      'gappsd.queue-delay-normal': 10,
      'gappsd.queue-delay-offline': 30,
      'gappsd.queue-warn-overflow': True,
      'gappsd.token-expiration': 86400,
    }

    self.__Load(config_file)
    self.__CheckForMissingKeys()

  def __Load(self, config_file):
    """Loads the configuration from the @p filename, and store it in the
    _data_string and _data_integer dictionaries."""
    parser = ConfigParser.RawConfigParser()
    parser.read([config_file])

    for key in self._data:
      self._data[key] = self.__LoadKey(parser, key, self._data[key])

  def __LoadKey(self, parser, key, default=None):
    """Tries to load a key from the configfile using the @p ConfigParser.
    Returns the default value on error. It doesn't catch parser errors."""
    try:
      value = parser.get(*(key.split(".", 1)))
    except ConfigParser.NoSectionError:
      return default
    except ConfigParser.NoOptionError:
      return default
    return value

  def __CheckForMissingKeys(self):
    """Checks that all the mandatory config options are present. Raises
    MissingValueError on error."""
    for key in self._data:
      if self._data[key] is None:
        raise MissingValueError, key

  def get_string(self, key):
    """ Returns the string config value for @p key."""
    return str(self._data[key])

  def get_int(self, key):
    """ Returns the integer config value for @p key."""
    return int(self._data[key])

  def set(self, key, value):
    """Updates the local configuration with the new (key, value) pair."""
    self._data[key] = value
