# /tmp/csm.conf
# DEBUG:
#     enabled: true
#     http_enabled: true
#     default_cluster_id: "bae4b468-565d-49df-9495-a43a5d89babc"
#
# # HTTPS setting for CSM Agent
# HTTPS:
#     port: 38409
#
#
# #Health
# HEALTH:
#     health_schema : ''
#     storage_actuator_request : '<CSM_PATH>/schema/storage_actuator_request.json'
#     node_actuator_request : '<CSM_PATH>/schema/node_actuator_request.json'



#!/usr/bin/env python3

import os
import sys

from cortx.utils.conf_store import Conf

# Loading configurations into confStore in-memory
Conf.load('csm_local', 'yaml:///tmp/csm.conf')

# Set a sample key, value to the configuration
Conf.set('csm_local', 'sample_conf>name', 'initialize')
# Getting above saved key,value
name = Conf.get('csm_local', 'sample_conf>name')
print(name)

# Set a boolean value to the configuration key
Conf.set('csm_local', 'sample_conf>status', False)
# Getting above saved key,value
status = Conf.get('csm_local', 'sample_conf>status')
print(status)

# set dict value to the configuration key
Conf.set('csm_local', 'sample_conf>payload', {'h-pos': 'no', 'v-pos': 'yes'})
# Getting above saved key,value
payload = Conf.get('csm_local', 'sample_conf>payload')
print(payload)

# Getting Boolean value
enabled_status_boolean = Conf.get('csm_local', 'DEBUG>enabled')
print(enabled_status_boolean)

# Getting numeric value
port_number = Conf.get('csm_local', 'HTTPS>port')
print(port_number)

# Getting Empty value will return ""
schema_value_when_empty = Conf.get('csm_local', 'HEALTH>health_schema')
print(schema_value_when_empty)

# Getting non existing key return None value
if_key_not_exist = Conf.get('csm_local', 'HEALTH>no_key_exist>schema')
print(if_key_not_exist)

# Delete a key, value from configuration
Conf.delete('csm_local', 'sample_conf>payload')
# Will print None now
payload = Conf.get('csm_local', 'sample_conf>payload')
print(payload)
