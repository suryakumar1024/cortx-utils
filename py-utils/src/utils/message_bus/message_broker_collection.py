#!/usr/bin/env python3

# CORTX-Py-Utils: CORTX Python common library.
# Copyright (c) 2021 Seagate Technology LLC and/or its Affiliates
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

import time
import json
import re
import errno

from cortx.utils.log import Log
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, \
    NewPartitions
from cortx.utils.message_bus.error import MessageBusError
from cortx.utils.message_bus.message_broker import MessageBroker
from cortx.utils.process import SimpleProcess
from cortx.utils import errors


class KafkaMessageBroker(MessageBroker):
    """ Kafka Server based message broker implementation """

    name = 'kafka'

    # Retention period in Milliseconds
    _default_msg_retention_period = 604800000
    _min_msg_retention_period = 1

    # Maximum retry count
    _max_config_retry_count = 3
    _max_purge_retry_count = 5
    _max_list_message_type_count = 15

    # Polling timeout
    _default_timeout = 0.5

    # Socket timeout
    _kafka_socket_timeout = 15000

    def __init__(self, broker_conf: dict):
        """ Initialize Kafka based Configurations """
        super().__init__(broker_conf)
        Log.init("MessageBusBroker", '/var/log/cortx/utils', level='INFO',
                 backup_count=5, file_size_in_mb=5)
        Log.info(f"KafkaMessageBroker: __init__(): initialized with broker"
                  f" configurations as {broker_conf}")
        self._clients = {'admin': {}, 'producer': {}, 'consumer': {}}

    def init_client(self, client_type: str, **client_conf: dict):
        """ Obtain Kafka based Producer/Consumer """
        Log.info(f"init_client(): Starting init_client() with"
                 f" client_type: {client_type}, **kwargs {client_conf}")
        """ Validate and return if client already exists """
        if client_type not in self._clients.keys():
            Log.error(f"init_client(): MessageBusError: Invalid client type "
                      f"{errors.ERR_INVALID_CLIENT_TYPE}, {client_type}")
            raise MessageBusError(errors.ERR_INVALID_CLIENT_TYPE, \
                "Invalid client type %s", client_type)

        if client_conf['client_id'] in self._clients[client_type].keys():
            if self._clients[client_type][client_conf['client_id']] != {}:
                # Check if message_type exists to send/receive
                client = self._clients[client_type][client_conf['client_id']]
                available_message_types = client.list_topics().topics.keys()
                if client_type == 'producer':
                    if client_conf['message_type'] not in \
                        available_message_types:
                        Log.error(f"KafkaException: init_client(): "
                            f"message_type {client_conf['message_type']} not"
                            f" found in {available_message_types} for "
                            f"{client_type}")
                        raise KafkaException(KafkaError(3))
                elif client_type == 'consumer':
                    if not any(each_message_type in available_message_types for\
                        each_message_type in client_conf['message_types']):
                        Log.error(f"KafkaException: init_client(): "
                            f"message_type {client_conf['message_type']} not"
                            f" found in {available_message_types} for "
                            f"{client_type}")
                        raise KafkaException(KafkaError(3))
                return

        kafka_conf = {}
        kafka_conf['bootstrap.servers'] = self._servers
        kafka_conf['client.id'] = client_conf['client_id']
        kafka_conf['error_cb'] = self._error_cb

        if client_type == 'admin' or self._clients['admin'] == {}:
            if client_type != 'consumer':
                kafka_conf['socket.timeout.ms'] = self._kafka_socket_timeout
                self.admin = AdminClient(kafka_conf)
                Log.info("init_client(): Successfully initialized"
                         " AdminClient()")
                self._clients['admin'][client_conf['client_id']] = self.admin

        if client_type == 'producer':
            producer = Producer(**kafka_conf)
            Log.info("init_client(): Successfully initialized"
                     " Producer()")
            self._clients[client_type][client_conf['client_id']] = producer

            self._resource = ConfigResource('topic', \
                client_conf['message_type'])
            conf = self.admin.describe_configs([self._resource])
            default_configs = list(conf.values())[0].result()
            for params in ['retention.ms']:
                if params not in default_configs:
                    Log.error(f"init_client(): MessageBusError: Missing "
                        f"required config parameter {params}. for client type"
                        f" {client_type}")
                    raise MessageBusError(errno.ENOKEY, \
                        "Missing required config parameter %s. for " +\
                        "client type %s", params, client_type)

            self._saved_retention = int(default_configs['retention.ms']\
                .__dict__['value'])

            # Set retention to default if the value is 1 ms
            self._saved_retention = self._default_msg_retention_period if \
                self._saved_retention == self._min_msg_retention_period else \
                int(default_configs['retention.ms'].__dict__['value'])

        elif client_type == 'consumer':
            for entry in ['offset', 'consumer_group', 'message_types', \
                'auto_ack', 'client_id']:
                if entry not in client_conf.keys():
                    Log.Error(f"init_client(): MessageBusError: Could not find"
                        f" entry {entry} in conf keys for client type"
                        f" {client_type}")
                    raise MessageBusError(errno.ENOENT, "Could not find " +\
                        "entry %s in conf keys for client type %s", entry, \
                        client_type)

            kafka_conf['enable.auto.commit'] = client_conf['auto_ack']
            kafka_conf['auto.offset.reset'] = client_conf['offset']
            kafka_conf['group.id'] = client_conf['consumer_group']

            consumer = Consumer(**kafka_conf)
            consumer.subscribe(client_conf['message_types'])
            Log.info(f"init_client(): Successfully initialized Consumer()"
                     f" and subscribed to {client_conf['message_types']}")
            self._clients[client_type][client_conf['client_id']] = consumer
            Log.info(f"init_client(): Successfully completed")

    def _task_status(self, tasks: dict, method: str):
        """ Check if the task is completed successfully """
        for task in tasks.values():
            try:
                task.result()  # The result itself is None
            except Exception as e:
                Log.Error(f"_task_status():MessageBusError: "
                    f"{errors.ERR_OP_FAILED}. Admin operation fails for"
                    f" {method}. {e}")
                raise MessageBusError(errors.ERR_OP_FAILED, \
                    "Admin operation fails for %s. %s", method, e)

    def _get_metadata(self, admin: object):
        """ To get the metadata information of message type """
        try:
            message_type_metadata = admin.list_topics().__dict__
            return message_type_metadata['topics']
        except KafkaException as e:
            Log.Error(f"_get_metadata(): MessageBusError:"
                f" {errors.ERR_OP_FAILED}. list_topics() failed. {e} Check "
                f"if Kafka service is running successfully")
            raise MessageBusError(errors.ERR_OP_FAILED, "list_topics() " +\
                "failed. %s. Check if Kafka service is running successfully", e)
        except Exception as e:
            Log.Error(f"_get_metadata(): MessageBusError:"
                      f" {errors.ERR_OP_FAILED}. list_topics() failed. {e} Check "
                      f"if Kafka service is running successfully")
            raise MessageBusError(errors.ERR_OP_FAILED, "list_topics() " + \
                "failed. %s. Check if Kafka service is running successfully", e)

    @staticmethod
    def _error_cb(err):
        """ Callback to check if all brokers are down """
        if err.code() == KafkaError._ALL_BROKERS_DOWN:
            raise MessageBusError(errors.ERR_SERVICE_UNAVAILABLE, \
                "Kafka service(s) unavailable. %s", err)

    def list_message_types(self, admin_id: str) -> list:
        """
        Returns a list of existing message types.

        Parameters:
        admin_id        A String that represents Admin client ID.

        Return Value:
        Returns list of message types e.g. ["topic1", "topic2", ...]
        """
        admin = self._clients['admin'][admin_id]
        return list(self._get_metadata(admin).keys())

    def register_message_type(self, admin_id: str, message_types: list, \
        partitions: int):
        """
        Creates a list of message types.

        Parameters:
        admin_id        A String that represents Admin client ID.
        message_types   This is essentially equivalent to the list of
                        queue/topic name. For e.g. ["Alert"]
        partitions      Integer that represents number of partitions to be
                        created.
        """
        Log.info(f"register_message_type(): started with arguments admin_id={admin_id}, message_types={message_types},"
            f" partitions={partitions}")
        admin = self._clients['admin'][admin_id]
        new_message_type = [NewTopic(each_message_type, \
            num_partitions=partitions) for each_message_type in message_types]
        created_message_types = admin.create_topics(new_message_type)
        self._task_status(created_message_types, method='register_message_type')

        for each_message_type in message_types:
            for list_retry in range(1, self._max_list_message_type_count+2):
                if each_message_type not in \
                    list(self._get_metadata(admin).keys()):
                    if list_retry > self._max_list_message_type_count:
                        Log.error(f"register_message_type(): MessageBusError:"
                                  f"Timed out after retry {list_retry} while "
                                  f"creating message_type {each_message_type}")
                        raise MessageBusError(errno.ETIMEDOUT, "Timed out " +\
                            "after retry %d while creating message_type %s.", \
                            list_retry, each_message_type)
                    time.sleep(list_retry*1)
                    continue
                else:
                    break
        Log.info(f"register_message_type(): Successfully completed")

    def deregister_message_type(self, admin_id: str, message_types: list):
        """
        Deletes a list of message types.

        Parameters:
        admin_id        A String that represents Admin client ID.
        message_types   This is essentially equivalent to the list of
                        queue/topic name. For e.g. ["Alert"]
        """
        Log.info(f"deregister_message_type(): Starting deregister_message_type()"
            f" with admin_id:{admin_id} and message_types:{message_types}")
        admin = self._clients['admin'][admin_id]
        deleted_message_types = admin.delete_topics(message_types)
        self._task_status(deleted_message_types, \
            method='deregister_message_type')

        for each_message_type in message_types:
            for list_retry in range(1, self._max_list_message_type_count+2):
                if each_message_type in list(self._get_metadata(admin).keys()):
                    if list_retry > self._max_list_message_type_count:
                        Log.error(f"deregister_message_type():MessageBusError:"
                            f" Timed out after {list_retry} retry to delete"
                            f" message_type {each_message_type}")
                        raise MessageBusError(errno.ETIMEDOUT, \
                            "Timed out after %d retry to delete message_type" +\
                            "%s.", list_retry, each_message_type)
                    time.sleep(list_retry*1)
                    continue
                else:
                    Log.info(f"deregister_message_type(): Successfully "
                        f"completed")
                    break

    def add_concurrency(self, admin_id: str, message_type: str, \
        concurrency_count: int):
        """
        Increases the partitions for a message type.

        Parameters:
        admin_id            A String that represents Admin client ID.
        message_type        This is essentially equivalent to queue/topic name.
                            For e.g. "Alert"
        concurrency_count   Integer that represents number of partitions to be
                            increased.

        Note:  Number of partitions for a message type can only be increased,
               never decreased
        """
        Log.info(f"add_concurrency(): starting with arguments admin_id:"
            f"{admin_id}, message_type:{message_type} "
            f"concurrency_count: {concurrency_count}")
        admin = self._clients['admin'][admin_id]
        new_partition = [NewPartitions(message_type, \
            new_total_count=concurrency_count)]
        partitions = admin.create_partitions(new_partition)
        self._task_status(partitions, method='add_concurrency')

        # Waiting for few seconds to complete the partition addition process
        for list_retry in range(1, self._max_list_message_type_count+2):
            if concurrency_count != len(self._get_metadata(admin)\
                [message_type].__dict__['partitions']):
                if list_retry > self._max_list_message_type_count:
                    Log.error(f"add_concurrency(): MessageBusError: Exceeded"
                        f" retry count {list_retry} for creating partitions"
                        f" for message_type {message_type}")
                    raise MessageBusError(errno.E2BIG, "Exceeded retry count" +\
                        " %d for creating partitions for message_type" +\
                        " %s.", list_retry, message_type)
                time.sleep(list_retry*1)
                continue
            else:
                Log.info(f"add_concurrency(): Successfully completed")
                break

    def send(self, producer_id: str, message_type: str, method: str, \
        messages: list, timeout=0.1):
        """
        Sends list of messages to Kafka cluster(s)

        Parameters:
        producer_id     A String that represents Producer client ID.
        message_type    This is essentially equivalent to the
                        queue/topic name. For e.g. "Alert"
        method          Can be set to "sync" or "async"(default).
        messages        A list of messages sent to Kafka Message Server
        """
        Log.log(f"send(): Start sending list of messages with arguments "
            f"messages: {messages}, producer_id: {producer_id}, "
            f"message_type: {message_type}, method: {method}")
        producer = self._clients['producer'][producer_id]
        if producer is None:
            Log.error(f"send(): MessageBusError: "
                f"{errors.ERR_SERVICE_NOT_INITIALIZED}. Producer "
                f"{producer_id} is not initialized")
            raise MessageBusError(errors.ERR_SERVICE_NOT_INITIALIZED,\
                "Producer %s is not initialized", producer_id)

        for message in messages:
            producer.produce(message_type, bytes(message, 'utf-8'))
            if method == 'sync':
                producer.flush()
            else:
                producer.poll(timeout=timeout)
        Log.log(f"send(): Successfully completed")

    def get_log_size(self, message_type: str):
        """ Gets size of log across all the partitions """
        total_size = 0
        cmd = "/opt/kafka/bin/kafka-log-dirs.sh --describe --bootstrap-server "\
            + self._servers + " --topic-list " + message_type
        Log.info(f"get_log_size(): Started with arguments message_type:"
            f" {message_type}")
        try:
            cmd_proc = SimpleProcess(cmd)
            run_result = cmd_proc.run()
            decoded_string = run_result[0].decode('utf-8')
            output_json = json.loads(re.search(r'({.+})', decoded_string).\
                group(0))
            for brokers in output_json['brokers']:
                partition = brokers['logDirs'][0]['partitions']
                for each_partition in partition:
                    total_size += each_partition['size']
            Log.info(f"get_log_size():Successfully completed")
            return total_size
        except Exception as e:
            Log.error(f"get_log_size(): MessageBusError:{errors.ERR_OP_FAILED}"
                f" Command {cmd} failed for message type {message_type} {e}")
            raise MessageBusError(errors.ERR_OP_FAILED, "Command %s failed" +\
                "for message type %s %s", cmd, message_type, e)

    def delete(self, admin_id: str, message_type: str):
        """
        Deletes all the messages from Kafka cluster(s)

        Parameters:
        message_type    This is essentially equivalent to the
                        queue/topic name. For e.g. "Alert"
        """
        admin = self._clients['admin'][admin_id]
        Log.info(f"delete(): Started with arguments admin_id: {admin_id},"
            f" message_type: {message_type}")
        
        for tuned_retry in range(self._max_config_retry_count):
            self._resource.set_config('retention.ms', \
                self._min_msg_retention_period)
            tuned_params = admin.alter_configs([self._resource])
            if list(tuned_params.values())[0].result() is not None:
                if tuned_retry > 1:
                    Log.error(f"delete(): MessageBusError: "
                        f"{errors.ERR_OP_FAILED} alter_configs() for resource"
                        f" {self._resource} failed using admin {admin} for "
                        f"message type {message_type}")
                    raise MessageBusError(errors.ERR_OP_FAILED, \
                        "alter_configs() for resource %s failed using admin" +\
                        "%s for message type %s", self._resource, admin,\
                        message_type)
                continue
            else:
                break

        for retry_count in range(1, (self._max_purge_retry_count + 2)):
            if retry_count > self._max_purge_retry_count:
                Log.error(f"delete(): MessageBusError: {errors.ERR_OP_FAILED}"
                    f" Unable to delete messages for message type "
                    f"{message_type} using admin {admin} after "
                    f"{retry_count} retries")
                raise MessageBusError(errors.ERR_OP_FAILED,\
                    "Unable to delete messages for message type %s using " +\
                    "admin %s after %d retries", message_type, admin,\
                    retry_count)
            time.sleep(0.1*retry_count)
            log_size = self.get_log_size(message_type)
            if log_size == 0:
                break

        for default_retry in range(self._max_config_retry_count):
            self._resource.set_config('retention.ms', self._saved_retention)
            default_params = admin.alter_configs([self._resource])
            if list(default_params.values())[0].result() is not None:
                if default_retry > 1:
                    Log.error(f"delete(): MessageBusError: {errno.ENOKEY} "
                        f"Unknown configuration for message type "
                        f"{message_type}.")
                    raise MessageBusError(errno.ENOKEY, "Unknown " +\
                        "configuration for message type %s.", message_type)
                continue
            else:
                break
        Log.info(f"delete(): Successfully completed.")

    def get_unread_count(self, message_type: str, consumer_group: str):
        """
        Gets the count of unread messages from the Kafka message server

        Parameters:
        message_type    This is essentially equivalent to the
                        queue/topic name. For e.g. "Alert"
        consumer_group  A String that represents Consumer Group ID.
        """
        table = []
        Log.info(f"get_unread_count(): Started with arguments message_type: "
            f"{message_type}, consumer_group: {consumer_group}")
        # Update the offsets if purge was called
        if self.get_log_size(message_type) == 0:
            cmd = "/opt/kafka/bin/kafka-consumer-groups.sh \
                --bootstrap-server " + self._servers + " --group " \
                + consumer_group + " --topic " + message_type + \
                " --reset-offsets --to-latest --execute"
            cmd_proc = SimpleProcess(cmd)
            res_op, res_err, res_rc = cmd_proc.run()
            if res_rc != 0:
                Log.error(f"get_unread_count(): MessageBusError: "
                    f"{errors.ERR_OP_FAILED}. Command {cmd} failed for "
                    f"consumer group {consumer_group}. {res_err}")
                raise MessageBusError(errors.ERR_OP_FAILED, "Command %s " +\
                    "failed for consumer group %s. %s", cmd, consumer_group,\
                    res_err)
            decoded_string = res_op.decode("utf-8")
            if 'Error' in decoded_string:
                Log.error(f"get_unread_count(): MessageBusError:"
                    f"{errors.ERR_OP_FAILED}. Command {cmd} failed for "
                    f"consumer group {consumer_group}. {res_err}")
                raise MessageBusError(errors.ERR_OP_FAILED, "Command %s" + \
                    " failed for consumer group %s. %s", cmd, \
                    consumer_group, res_err)
        cmd = "/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server "\
            + self._servers + " --describe --group " + consumer_group
        cmd_proc = SimpleProcess(cmd)
        res_op, res_err, res_rc = cmd_proc.run()
        if res_rc != 0:
            Log.error(f"get_unread_count(): MessageBusError: "
                f"{errors.ERR_OP_FAILED}. command {cmd} failed for consumer"
                f" group {consumer_group}. {res_err}.")
            raise MessageBusError(errors.ERR_OP_FAILED, "command %s " + \
                "failed for consumer group %s. %s.", cmd, consumer_group, \
                res_err)
        decoded_string = res_op.decode("utf-8")
        if decoded_string == "":
            Log.error(f"get_unread_count(): MessageBusError: {errno.ENOENT}."
                f" No active consumers in the consumer group, "
                f"{consumer_group}.")
            raise MessageBusError(errno.ENOENT, "No active consumers" +\
                " in the consumer group, %s.", consumer_group)
        elif 'Error' in decoded_string:
            Log.error(f"get_unread_count(): {errors.ERR_OP_FAILED} command "
                f"{cmd} failed for consumer group {consumer_group}. "
                f"{decoded_string}.")
            raise MessageBusError(errors.ERR_OP_FAILED, "command %s " +\
                "failed for consumer group %s. %s.", cmd, consumer_group,\
                decoded_string)
        else:
            split_rows = decoded_string.split("\n")
            rows = [row.split(' ') for row in split_rows if row != '']
            for each_row in rows:
                new_row = [item for item in each_row if item != '']
                table.append(new_row)
            message_type_index = table[0].index('TOPIC')
            lag_index = table[0].index('LAG')
            unread_count = [int(lag[lag_index]) for lag in table if \
                lag[lag_index] != 'LAG' and lag[lag_index] != '-' and \
                lag[message_type_index] == message_type]

            if len(unread_count) == 0:
                Log.error(f"get_unread_count(): MessageBusError: "
                    f"{errno.ENOENT}. No active consumers in the consumer"
                    f" group, {consumer_group}.")
                raise MessageBusError(errno.ENOENT, "No active " +\
                    "consumers in the consumer group, %s.", consumer_group)
        Log.info(f"get_unread_count(): Successfully completed.")
        return sum(unread_count)

    def receive(self, consumer_id: str, timeout: float = None) -> list:
        """
        Receives list of messages from Kafka Message Server

        Parameters:
        consumer_id     Consumer ID for which messages are to be retrieved
        timeout         Time in seconds to wait for the message. Timeout of 0
                        will lead to blocking indefinitely for the message
        """
        consumer = self._clients['consumer'][consumer_id]
        Log.info(f"receive(): Started with arguments consumer_id: "
            f"{consumer_id}, timeout: {timeout}")
        if consumer is None:
            Log.error(f"receive(): MessageBusError: "
                f"{errors.ERR_SERVICE_NOT_INITIALIZED} Consumer {consumer_id}"
                f" is not initialized.")
            raise MessageBusError(errors.ERR_SERVICE_NOT_INITIALIZED, \
                "Consumer %s is not initialized.", consumer_id)

        if timeout is None:
            timeout = self._default_timeout

        try:
            while True:
                msg = consumer.poll(timeout=timeout)
                if msg is None:
                    # if blocking (timeout=0), NoneType messages are ignored
                    if timeout > 0:
                        return None
                elif msg.error():
                    Log.error(f"receive(): MessageBusError: "
                        f"{errors.ERR_OP_FAILED} poll({timeout}) for consumer"
                        f" {consumer_id} failed to receive message. "
                        f"{msg.error()}")
                    raise MessageBusError(errors.ERR_OP_FAILED, "poll(%s) " +\
                        "for consumer %s failed to receive message. %s", \
                        timeout, consumer_id, msg.error())
                else:
                    return msg.value()
        except KeyboardInterrupt:
            Log.error(f"receive(): MessageBusError: {errno.EINTR} Received"
                f" Keyboard interrupt while trying to receive message for"
                f" consumer {consumer_id}")
            raise MessageBusError(errno.EINTR, "Received Keyboard interrupt " +\
                "while trying to receive message for consumer %s", consumer_id)

    def ack(self, consumer_id: str):
        """ To manually commit offset """
        consumer = self._clients['consumer'][consumer_id]
        if consumer is None:
            Log.error(f"ack(): MessageBusError: "
                f"{errors.ERR_SERVICE_NOT_INITIALIZED} Consumer "
                f"{consumer_id} is not initialized.")
            raise MessageBusError(errors.ERR_SERVICE_NOT_INITIALIZED,\
                "Consumer %s is not initialized.", consumer_id)
        consumer.commit(async=False)
