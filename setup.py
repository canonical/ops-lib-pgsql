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
import setuptools


def get_long_description():
    with open("README.md", "r") as fh:
        return fh.read()


def get_version():
    with open(os.path.join("pgsql", "__init__.py"), "r") as fh:
        pkg = fh.read()

    LIBAPI = int(re.search(r"""(?m)^LIBAPI\s*=\s*(\d+)""", pkg).group(1))
    LIBPATCH = int(re.search(r"""(?m)^LIBPATCH\s*=\s*(\d+)""", pkg).group(1))
    return f"{LIBAPI}.{LIBPATCH}"


setuptools.setup(
    name="ops-lib-pgsql",
    version=get_version(),
    author="Stuart Bishop",
    author_email="stuart.bishop@canonical.com",
    maintainer="PostgreSQL Charmers",
    maintainer_email="postgresql-charmers@lists.launchpad.net",
    description="PostgreSQL database relation for Juju Operator Framework Charms",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    url="https://github.com/canonical/ops-lib-pgsql",
    packages=["pgsql", "pgsql.opslib.pgsql"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Operating System :: OS Independent",
    ],
    keywords="juju charm opslib postgres postgresql pgsql",
    project_urls={
        "Juju": "https://juju.is/",
        "Juju Operator Framework": "https://pypi.org/project/ops/",
    },
    python_requires=">=3.6",
    install_requires=["ops >= 0.8.0", "PyYAML", "pgconnstr"],
)
