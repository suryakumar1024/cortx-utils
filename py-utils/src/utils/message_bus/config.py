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

class Config():
    def __init__(self, filename=None):
        if filename is not None:
            self.file_handle = filename
        else:
            self.config = {
                'message_broker': "kafka",
                'message_type': [{'name': "Alert", 'replication_factor': 3, 'policy': "Remove_on_ACK"}],
                'message_server': [{'bootstrap.servers': 'localhost:9092'}],
                'producer': [{'bootstrap.servers': 'localhost:9092'}],
                'consumer': [
                    {
                        'bootstrap.servers': 'localhost:9092',
                        'group.id': 'sspl',
                        'auto.offset.reset': 'earliest',
                        'enable.auto.commit': False
                    }
                ]
            }

    def get_config(self):
        return self.config


class KafkaConfig(Config):
    def __init__(self):
        #Read from file
        self.config = {
            'bus': "kafka-python",
            'topics': [{'name': "Alert", 'replication_factor': 3, 'policy': "Remove_on_ACK"}],
            'client': [{'bootstrap_servers': 'localhost:9092'}],
            'producer': [{'bootstrap_servers': 'localhost:9092'}],
            'consumer': [{'bootstrap_servers': 'localhost:9092', 'auto_offset_reset': 'earliest', 'consumer_timeout_ms': 1000}]
        }

    def get_config(self):
        return self.config

class ConfluentKafkaConfig(Config):
    def __init__(self):
        self.config = {
            'bus': "confluent-kafka",
            'topics': [{'name': "Alert", 'replication_factor': 3, 'policy': "Remove_on_ACK"}],
            'client': [{'bootstrap.servers': 'localhost:9092'}],
            'producer': [{'bootstrap.servers': 'localhost:9092'}],
            'consumer': [{'bootstrap.servers': 'localhost:9092','session.timeout.ms': 6000,
            'group.id':'default'}]
        }
    def get_config(self):
        #Read from file
        return self.config