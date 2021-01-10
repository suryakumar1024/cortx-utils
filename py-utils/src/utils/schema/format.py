#!/bin/python3

# CORTX Python common library.
# Copyright (c) 2020 Seagate Technology LLC and/or its Affiliates
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.

import errno
import re
import sys
import inspect
import json
import yaml
import toml


class FormatError(Exception):
    """ Generic Exception with error code and output """

    def __init__(self, rc, message, *args):
        self._rc = rc
        self._desc = message % (args)

    def __str__(self):
        if self._rc == 0: return self._desc
        return "error(%d): %s" %(self._rc, self._desc)


class Format:
    """ Represents a format converter """
    pass


class JsonFormat(Format):
    """ Json Format Handler """
    name = "json"

    def dump(self, data: dict) -> str:
        return json.dumps(data)


class YamlFormat(Format):
    """ YAML Format Handler """
    name = "yaml"

    def dump(self, data: dict) -> str:
        return yaml.dump(data)


class TomlFormat(Format):
    """ YAML Format Handler """
    name = "toml"

    def dump(self, data: dict) -> str:
        return toml.dumps(data)


class Formatter:
    _formats = {}

    def dump(data: dict, format_type) -> str:
        if format_type not in Formatter._formats.keys():
            members = inspect.getmembers(sys.modules[__name__])
            for name, cls in members:
                if name == "Format" or not name.endswith("Format"):
                    continue
                if cls.name == format_type:
                    Formatter._formats[format_type] = cls()

        if format_type not in Formatter._formats.keys():
            raise FormatError(errno.EINVAL, "Invalid format type %s", format_type)

        return Formatter._formats[format_type].dump(data)


if __name__ == "__main__":
    str = Formatter.dump({"k1": "v1"}, "json")
    print(str)
