#!/usr/bin/env python3

# CORTX-Py-Utils: CORTX Python common library.
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

from datetime import datetime, timedelta


class Notifier():
    def __init__(self):
        pass

    def add(self, messages):
        li = []
        for each in messages:
            to_dict = dict(each._asdict())
            to_dict['value'] += b' added'
            li.append(to_dict)
        return li

    def last_one_hour_data(self, messages):
        li = []
        for each in messages:
            to_dict = dict(each._asdict())
            to_dict['timestamp'] = datetime.fromtimestamp(to_dict['timestamp'] / 1000.0).strftime('%Y-%m-%d %H:%M:%S')
            last_hour = (datetime.now() - timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S')
            if to_dict['timestamp'] > last_hour:
                li.append(to_dict)
        return li

    def check_topic(self, topic, messages):
        if 'testing' in topic:
            return True
        else:
            return False