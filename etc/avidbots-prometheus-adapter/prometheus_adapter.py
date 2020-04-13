#!/usr/bin/env python
#   ______                   __  __              __
#  /\  _  \           __    /\ \/\ \            /\ \__
#  \ \ \L\ \  __  __ /\_\   \_\ \ \ \____    ___\ \ ,_\   ____
#   \ \  __ \/\ \/\ \\/\ \  /'_` \ \ '__`\  / __`\ \ \/  /',__\
#    \ \ \/\ \ \ \_/ |\ \ \/\ \L\ \ \ \L\ \/\ \L\ \ \ \_/\__, `\
#     \ \_\ \_\ \___/  \ \_\ \___,_\ \_,__/\ \____/\ \__\/\____/
#      \/_/\/_/\/__/    \/_/\/__,_ /\/___/  \/___/  \/__/\/___/
#  Copyright 2020, Avidbots Corp
#  @name    prometheus_adapter.py
#  @brief   Upload selected prometheus data to external database
#  @author  Camila Perez-Gavilan <camila.gavilan@avidbots.com>
#

import yaml
import json
import urllib
import urllib2
import boto3
import sys
from decimal import Decimal

configs=""
upload_interval = 10
nodeaddr = "http://localhost:9091"
dynamodb = None
table = None

# Make a request to the prometheus api
#
# @param node_address The address of the main prometheus node hosts the api (ex. http://localhost:9091)
# @param data A dictionary of keys/values to send along as well
#
# @return (status, content) status is true if successful, content is either the query response or an error message
def query_prometheus(node_address, query):
    request_string = node_address + "/api/v1/query?query=" + query + "[" + str(upload_interval) + "m]"
    #request_string = node_address + "/api/v1/query?query=" + query 
    print(request_string)
    try:
        request = urllib2.Request(request_string)
        receive = urllib2.urlopen(request, timeout=5)
        error_code = receive.getcode()
        #content_type = receive.info().getheader("content-type")
    except Exception as e:
        return (False, str(e))
    if error_code != 200:
        return (False,"Error code: " + str(error_code) + " , " + "Server returned an error")
    chunk = True
    response = ""
    while chunk:
        try:
            chunk = receive.read()
        except Exception as e:
            return (False,"Error code: " + str(error_code) + " , " + str(e))
        if not chunk:
            break
        response += chunk
    receive.close()
    response = json.loads(response)
    if response['status'] == 'success':
        return (True, response['data'])
    return (False, "Error type: " + response['errorType'] + " , Error: " + response['error'])

def read_config():
    global configs
    try:
        with open(r'/etc/avidbots-prometheus-adapter/prometheus_adapter.yaml') as file:
            configs = yaml.load(file)
            upload_interval = configs
    except Exception as e:
        exit_with_error("Error reading configuration file: " + str(e))
        return False
    return True
            
def get_metrics_list():
    metrics = []
    try:
        for node in configs["nodes"]:
            for metric in configs["nodes"][node]["metrics"]:
                metrics.append(metric)
    except Exception as e:
        exit_with_error("Failed to obtain metrics list: " + str(e))
    return metrics
    
def init_database():
    global dynamodb
    global table
    try:
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('robot_metrics')
    except Exception as e:
        exit_with_error("Error initializing database: " + str(e))
    return True

def prom_query_to_dynamo_items(query_out):
    items = []
    try:
        for item in query_out["result"]:
            d_it = {}
            d_it["metric"] = item["metric"]
            d_it["metric_name"] = item["metric"]["__name__"]
            # test values for robot number and sw version
            d_it["robot"] = 219
            d_it["sw_version"] = "prometheus_test"
            if "value" in item:
                # vector metric
                d_it["timestamp"] = Decimal(item["value"][0])
                d_it["value"] = item["value"][1]
                items.append(dict(d_it))
            if "values" in item:
                # matrix metric
                for value in item["values"]:
                    d_it["timestamp"] = Decimal(value[0])
                    d_it["value"] = value[1]
                    items.append(dict(d_it))
                    
    except Exception as e:
        print str(e)
    #print("database upload")
    return items

def upload_dynamo_item(d_item):
    ret = table.put_item(Item=d_item)
    if ret["ResponseMetadata"]["HTTPStatusCode"] == 200:
        return True
    print("[Prometheus Adapter]: Error uploading item to database: " + ret)
    return False

def exit_with_error(error_message):
    print ("[Prometheus Adapter]: " + error_message)
    sys.exit(1)

if __name__  == '__main__':
    read_config()
    init_database()
    metrics_list = get_metrics_list()
    print(metrics_list)
    for metric in metrics_list:
        print(metric)
        (status, json_response) = query_prometheus(nodeaddr,metric)
        if status:
            its = prom_query_to_dynamo_items(json_response)
            for d_it in its:
                success = upload_dynamo_item(d_it)
        else:
             print("[Prometheus Adapter]: Error querying prometheus: " + json_response)



