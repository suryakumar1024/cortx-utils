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


class BusClient():
    def __init__(self, busHandle, role):
        self.role = role
        self.busHandle = busHandle
        self.bus_client = self.busHandle.create(self.role)

    def send(self, message=None):
        self.busHandle.send(self.bus_client, message)

    def receive(self, consumer):
        return self.busHandle.receive(consumer)

    def create(self):
        self.busHandle.create(self.role)

    def subscribe(self, topic, notifier):
        # self.mapper[]
        return self.busHandle.subscribe(self.bus_client, topic, notifier)
    
    def unsubscribe(self, subscription):
        return self.busHandle.unsubscribe(subscription)