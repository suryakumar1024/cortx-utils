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


from src.utils.message_bus.bus import BusClient
from src.utils.message_bus.functor import Callable

class Consumer(BusClient):

    def __init__(self, busHandle, message_type=None):
        super().__init__(busHandle, 'CONSUMER', message_type)

    def send(self):
        pass

    def subscribe(self, topic, func_call=None):
        if func_call is not None:
            notifier = Callable(topic, func_call)
        else:
            notifier = None
        return super().subscribe(topic, notifier)

    def unsubscribe(self, subscription):
        return super().unsubscribe(subscription)

    def receive(self):
        return super().receive()
        #return super().receive(consumer)