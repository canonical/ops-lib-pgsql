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

import functools
import ipaddress
import re
from typing import Any, Iterable, Tuple
import urllib.parse


@functools.total_ordering
class ConnectionString:
    r"""A libpq connection string.

    >>> c = ConnectionString(host='1.2.3.4', dbname='mydb', port=5432, user='anon',
    ...                      password="sec'ret", application_name='myapp')
    ...
    >>> print(str(c))
    application_name=myapp dbname=mydb host=1.2.3.4 password=sec\'ret port=5432 user=anon
    >>> print(str(ConnectionString(str(c), dbname='otherdb')))
    application_name=myapp dbname=otherdb host=1.2.3.4 password=sec\'ret port=5432 user=anon

    Components may be accessed as attributes.

    >>> c.dbname
    'mydb'
    >>> c.host
    '1.2.3.4'
    >>> c.port
    '5432'

    Standard components will default to None if not explicitly set. See
    https://www.postgresql.org/docs/12/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    for the list of standard keywords.

    >>> c.connect_timeout is None
    True

    The standard URI format is also accessible:

    >>> print(c.uri)
    postgresql://anon:sec%27ret@1.2.3.4:5432/mydb?application_name=myapp

    >>> print(ConnectionString(c, host='2001:db8::1234').uri)
    postgresql://anon:sec%27ret@[2001:db8::1234]:5432/mydb?application_name=myapp

    """
    conn_str: str = None  # libpq connection string, as returned by __str__()

    uri: str = None  # libpq connection URI (or URL, but PostgreSQL docs refer to URI)

    # libpq connection string elements, per
    # https://www.postgresql.org/docs/12/libpq-connect.html#LIBPQ-PARAMKEYWORDS
    host: str = None
    hostaddr: str = None
    port: str = None
    dbname: str = None
    user: str = None
    password: str = None
    passfile: str = None
    connect_timeout: str = None
    client_encoding: str = None
    options: str = None
    application_name: str = None
    fallback_application_name: str = None
    keepalives: str = None
    keepalives_idle: str = None
    keepalives_interval: str = None
    keepalives_count: str = None
    tcp_user_timeout: str = None
    tty: str = None
    replication: str = None
    gssencmode: str = None
    sslmode: str = None
    requiressl: str = None
    sslcompression: str = None
    sslcert: str = None
    sslkey: str = None
    sslrootcert: str = None
    sslcrl: str = None
    requirepeer: str = None
    krbsrvname: str = None
    gsslib: str = None
    service: str = None
    target_session_attrs: str = None

    def __init__(self, conn_str: str = None, **kw):  # noqa
        # Parse libpq key=value style connection string. Components
        # passed by keyword argument override. If the connection string
        # is invalid, some components may be skipped (but in practice,
        # where database and usernames don't contain whitespace,
        # quotes or backslashes, this doesn't happen).
        def quote(x: Any):
            q = str(x).replace("\\", "\\\\").replace("'", "\\'")
            q = q.replace("\n", " ")  # \n is invalid in connection strings
            if " " in q:
                q = "'" + q + "'"
            return q

        def dequote(x: Any):
            q = str(x).replace("\\'", "'").replace("\\\\", "\\")
            return q

        if conn_str is not None:
            r = re.compile(
                r"""(?x)
                    (\w+) \s* = \s*
                    (?:
                      '((?:.|\.)*?)' |
                      (\S*)
                    )
                    (?=(?:\s|\Z))
                """
            )
            for key, v1, v2 in r.findall(conn_str):
                if key not in kw:
                    kw[key] = dequote(v1 or v2)

        c = " ".join("{}={}".format(k, quote(v)) for k, v in sorted(kw.items()) if v)
        self.conn_str = c

        for k, v in kw.items():
            setattr(self, k, v)

        self._keys = set(kw.keys())

        # Construct the documented PostgreSQL URI for applications
        # that use this format. PostgreSQL docs refer to this as a
        # URI so we do do, even though it meets the requirements the
        # more specific term URL.
        fmt = ["postgresql://"]
        d = {k: urllib.parse.quote(str(v), safe="") for k, v in kw.items() if v}
        if "user" in d:
            if "password" in d:
                fmt.append("{user}:{password}@")
            else:
                fmt.append("{user}@")
        if "host" in kw:
            try:
                hostaddr = ipaddress.ip_address(kw.get("hostaddr") or kw.get("host"))
                if isinstance(hostaddr, ipaddress.IPv6Address):
                    d["hostaddr"] = "[{}]".format(hostaddr)
                else:
                    d["hostaddr"] = str(hostaddr)
            except ValueError:
                # Not an IP address, but hopefully a resolvable name.
                d["hostaddr"] = d["host"]
            del d["host"]
            fmt.append("{hostaddr}")
        if "port" in d:
            fmt.append(":{port}")
        if "dbname" in d:
            fmt.append("/{dbname}")
        main_keys = frozenset(["user", "password", "dbname", "hostaddr", "port"])
        extra_fmt = ["{}={{{}}}".format(extra, extra) for extra in sorted(d.keys()) if extra not in main_keys]
        if extra_fmt:
            fmt.extend(["?", "&".join(extra_fmt)])
        self.uri = "".join(fmt).format(**d)

    def keys(self) -> Iterable[str]:
        return iter(self._keys)

    def items(self) -> Iterable[Tuple[str, str]]:
        return {k: self[k] for k in self.keys()}.items()

    def values(self) -> Iterable[str]:
        return iter(self[k] for k in self.keys())

    def __getitem__(self, key: str) -> str:
        if isinstance(key, int):
            return super(ConnectionString, self).__getitem__(key)
        try:
            return getattr(self, key)
        except AttributeError:
            raise KeyError(key)

    def __str__(self) -> str:
        return self.conn_str

    def __repr__(self) -> str:
        return "ConnectionString({!r})".format(self.conn_str)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, ConnectionString):
            return NotImplemented
        return self.conn_str == other.conn_str

    def __lt__(self, other: Any) -> bool:
        if not isinstance(other, ConnectionString):
            return NotImplemented
        return self.conn_str < other.conn_str
