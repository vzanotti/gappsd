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

"""Implements a python representation of the SQL version of the Google Apps
user accounts. It enables easy manipulation of the different informations"""

class AccountContentError(Exception):
  """Indicates that an invalid content was found in an Account. For example,
  an account not indexed by its account_name will raise this exception."""
  pass

class AccountActionError(Exception):
  """Indicates an error while /doing/ something to an account. For example,
  creating an account in the SQL database without providing all the required
  fields will raise this exception."""
  pass


def LoadAccountFromDatabase(sql, account_name):
  """Loads an account (indexed by the @p account_name) from the database,
  a returns the corresponding Account object."""

  result = sql.Query("SELECT * FROM gapps_accounts WHERE g_account_name = %s",
                     (account_name,))
  if result is None or not len(result):
    return None
  return Account(account_name, result[0])


class Account(object):
  """Represents an account as in the database.

  Example usage:
    account = Account.loadFromDatabase(sql_object, "<account name>")
    account.set("last_name", "Foo-Bar")
    account.Update(sql_object)

    account = Account("<account name>")
    account.set("last_name", "Foo")
    ...
    account.Create(sql_object)
  """

  # Accounts statuses.
  STATUS_UNPROVISIONED = "unprovisioned"
  STATUS_DISABLED = "disabled"
  STATUS_ACTIVE = "active"

  # List of offered data fields.
  #   Format: <field name>: [<modifier>, <mandatory>, <readonly>]
  _DATA_FIELDS = {
    "g_account_id":   [None, False, False],
    "g_account_name": [None, True,  True],
    "g_first_name":   [None, True,  False],
    "g_last_name":    [None, True,  False],
    "g_status":       [None, False, False],
    "g_admin":        [bool, False, False],
    "g_suspension":   [None, False, False],
    "r_disk_usage":   [None, False, False],
    "r_creation":     [None, False, False],
    "r_last_login":   [None, False, False],
    "r_last_webmail": [None, False, False],
  }

  def __init__(self, account_name, account_dict=None):
    if account_dict is None:
      account_dict = {}

    self._data = {"g_account_name": account_name}
    self._data_changed = {}

    if "g_account_name" in account_dict:
      if account_dict["g_account_name"] != self._data["g_account_name"]:
        raise AccountContentError( \
          "Got different account names from two sources.")

    for (key, (modifier, mandatory, ro)) in list(self._DATA_FIELDS.items()):
      if key in account_dict:
        if modifier is None or account_dict[key] is None:
          if isinstance(account_dict[key], str):
            self._data[key] = account_dict[key].decode("utf8")
          else:
            self._data[key] = account_dict[key]
        else:
          self._data[key] = modifier(account_dict[key])

  # Data accessors / mutators.
  def set(self, key, value):
    """Updates the value of one of the Account's data fields. Modifying
    read-only or non-existant fields raise an AccountActionError."""

    if not key in self._DATA_FIELDS or self._DATA_FIELDS[key][2]:
      raise AccountActionError("Non-existent/Read-Only field '%s'" % key)
    if not key in self._data or self._data[key] != value:
      self._data_changed[key] = True
    self._data[key] = value

  def get(self, key):
    """Retrieves the value of one of the Account's data fields."""

    return self._data[key] if key in self._data else None

  # Account manipulation.
  def Create(self, sql):
    """Commits the current representation of the Account (ie. this object)
    to the database, as a new account. Fails if the account already existed."""

    if LoadAccountFromDatabase(sql, self.get("g_account_name")) != None:
      raise AccountActionError( \
        "Cannot create account, as it already exists in the database.")

    data = {}
    for (key, (modifier, mandatory, ro)) in list(self._DATA_FIELDS.items()):
      if mandatory and not key in self._data:
        raise AccountActionError("Missing field '%s' for create." % key)
      if key in self._data:
        data[key] = self._data[key]

    sql.Insert("gapps_accounts", data)

  def Update(self, sql):
    """Updates the SQL version of the account, using values updated through the
    set() method of the object."""

    changed_data = {}
    for (key, (modifier, mandatory, ro)) in list(self._DATA_FIELDS.items()):
      if key in self._data_changed and self._data_changed[key]:
        changed_data[key] = self._data[key]

    if len(changed_data):
      sql.Update("gapps_accounts",
                 changed_data,
                 {"g_account_name": self._data["g_account_name"]})

  def Delete(self, sql):
    """Deletes the SQL version of the account."""

    sql.Execute(
      "DELETE FROM gapps_accounts WHERE g_account_name = %s",
      (self._data["g_account_name"],))
