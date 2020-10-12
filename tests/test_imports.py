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

import os.path
import re
import unittest

import ops.lib


class TestImports(unittest.TestCase):
    VERSION = 1

    def test_python_standard(self):
        # Test standard Python import mechanism.
        import pgsql

        pgsql.ConnectionString
        pgsql.MasterAvailableEvent
        pgsql.PostgreSQLClient

    def test_ops_lib_use(self):
        # Test recommended ops.lib.use import mechanism.
        pgsql = ops.lib.use("pgsql", self.VERSION, "postgresql-charmers@lists.launchpad.net")
        pgsql.ConnectionString
        pgsql.MasterAvailableEvent
        pgsql.PostgreSQLClient

    def test_protocol_version(self):
        import pgsql

        self.assertEqual(
            pgsql.LIBAPI,
            self.VERSION,
            "LIBAPI in __init__.py does not match test version, got {pgsql.LIBAPI}, want {self.VERSION}",
        )

    def test_setup_version(self):
        with open(os.path.join(os.path.dirname(__file__), os.pardir, "setup.py"), "r") as s:
            m = re.search(r"""version\s*=\s*"(\d+)\.""", s.read())
        self.assertIsNotNone(m, "version not found in setup.py")
        v = int(m.group(1))
        self.assertEqual(
            v, self.VERSION, f"version in setup.py does not match test version, got {v}, want {self.VERSION}"
        )
