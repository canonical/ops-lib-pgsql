# This file is part of the ops-lib-pgsql component for Juju Operator
# Framework Charms.
# Copyright 2020 Canonical Ltd.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the Lesser GNU General Public License version 3,
# as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranties of
# MERCHANTABILITY, SATISFACTORY QUALITY, or FITNESS FOR A PARTICULAR
# PURPOSE.  See the Lesser GNU General Public License for more details.
#
# You should have received a copy of the Lesser GNU General Public
# License along with this program.  If not, see
# <http://www.gnu.org/licenses/>.

import unittest
import unittest.mock

import ops.charm
import ops.lib
import ops.testing

from pgsql import pgsql as _pgsql


pgsql = ops.lib.use("pgsql", 1, "postgresql-charmers@lists.launchpad.net")


class Charm(ops.charm.CharmBase):
    def __init__(self, *args, **kw):
        super().__init__(*args, **kw)
        self.db = pgsql.PostgreSQLClient(self, 'db')


class TestPGSQL(unittest.TestCase):
    def setUp(self):
        self.leadership_data = {}
        lp = unittest.mock.patch.multiple(
            _pgsql,
            _get_pgsql_leader_data=self.leadership_data.copy,
            _set_pgsql_leader_data=self.leadership_data.update
        )
        lp.start()
        self.addCleanup(lp.stop)

        self.harness = ops.testing.Harness(Charm)
        self.addCleanup(self.harness.cleanup)

    def testLeadershipMock(self):
        self.leadership_data['foo'] = 'bar'
        self.assertEqual(_pgsql._get_pgsql_leader_data(), self.leadership_data)
        self.assertIsNot(_pgsql._get_pgsql_leader_data(), self.leadership_data)

        _pgsql._set_pgsql_leader_data({'one': 'two'})
        self.assertEqual(_pgsql._get_pgsql_leader_data(), {'foo': 'bar', 'one': 'two'})

        _pgsql._set_pgsql_leader_data({'foo': 'baz'})
        self.assertEqual(_pgsql._get_pgsql_leader_data(), {'foo': 'baz', 'one': 'two'})

        self.assertEqual(self.leadership_data, {'foo': 'baz', 'one': 'two'})
