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
import subprocess
import sys
import unittest

import ops.lib

import pgsql


class TestImports(unittest.TestCase):
    def test_python_standard(self):
        # Test standard Python import mechanism.
        pgsql.ConnectionString
        pgsql.MasterAvailableEvent
        pgsql.PostgreSQLClient

    def test_ops_lib_use(self):
        # Test recommended ops.lib.use import mechanism.
        _pgsql = ops.lib.use("pgsql", pgsql.LIBAPI, "postgresql-charmers@lists.launchpad.net")
        _pgsql.ConnectionString is pgsql.ConnectionString
        _pgsql.MasterAvailableEvent is pgsql.MasterAvailableEvent
        _pgsql.PostgreSQLClient is pgsql.PostgreSQLClient

    def test_setup_version(self):
        setup_py = os.path.join(os.path.dirname(__file__), os.pardir, "setup.py")
        cmd = [sys.executable, setup_py, "--version"]
        ver = subprocess.check_output(cmd, universal_newlines=True).strip()

        self.assertEqual(
            ver,
            f"{pgsql.LIBAPI}.{pgsql.LIBPATCH}",
            "version reported by setup.py does not match ops.lib version in pgsql/__init__.py",
        )
