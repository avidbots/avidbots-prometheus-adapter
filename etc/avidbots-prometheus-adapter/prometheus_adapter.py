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
import sys
import subprocess
from decimal import Decimal

configs=""
upload_interval = 10
nodeaddr = "http://localhost:9091"
dynamodb = None
table = None

"""
Utility function to run multiple commands with subprocess
commands = string holding all commands to process, separated by "; "
return (stdout, stderr)
"""
def subprocess_cmd(commands):
    process = subprocess.Popen('/bin/bash', shell=False, universal_newlines=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = process.communicate(commands)
    return (out, err)


def make_request(req_str):
    try:
        request = urllib2.Request(req_str)
        receive = urllib2.urlopen(request, timeout=5)
        error_code = receive.getcode()
        content_type = receive.info().getheader("content-type")
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
    if "json" in content_type:
        return (True, json.loads(response))
    return (True,response)


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
    (status, response) = make_request(request_string)
    if status == False:
        return (status,response)
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


"""
@return robot_name, which is the computer's hostname 
"""
def get_robot_name():
    (out,err) = subprocess_cmd("hostname")
    return out.strip()

"""
@return sw_version string if there is one installed, "none" otherwise
"""
def get_sw_version():
    (out,err) = subprocess_cmd("avidbots-version")
    if out == '':
        return "none"
    elif "unknown software package" in out:
        return "none"
    return out.strip()


def prom_query_to_elastic_items(query_out):
    items=[]
    try:
        for item in query_out["result"]:
            d_it = {}
            d_it["metric_name"] = item["metric"]["__name__"]
            for dkey in item["metric"]:
                if dkey != "__name__":
                    d_it[dkey] = item["metric"][dkey]
            d_it["robot"] = get_robot_name()
            d_it["sw_version"] = get_sw_version()
            if "value" in item:
                # vector metric
                d_it["timestamp"] = float(item["value"][0])
                d_it["value"] = item["value"][1]
                items.append(dict(d_it))
            if "values" in item:
                # matrix metric
                for value in item["values"]:
                    d_it["timestamp"] = float(value[0])
                    d_it["value"] = value[1]
                    items.append(dict(d_it))
                    
    except Exception as e:
        print str(e)
    return items


def conditional_encode(s, encoding='utf-8', errors='strict'):
    if isinstance(s, unicode):
        return s.encode(encoding, errors)
    return s

def upload_elastic_item(d_item):
    d_item = {conditional_encode(k): conditional_encode(v) for k, v in d_item.iteritems()}
    es_endpoint = "https://search-avidbots-test-wynhsbbpr67ldthb4mxbrkwmsy.us-east-1.es.amazonaws.com/"
    curl_str = "curl -X POST " + es_endpoint + d_item["metric_name"] + "/_doc -d '" + json.dumps(d_item) + "' -H 'Content-Type: application/json'"
    (out,err) = subprocess_cmd(curl_str)
    if "\"result\":\"created\"" in out:
        return True
    return False   
    

def exit_with_error(error_message):
    print ("[Prometheus Adapter]: " + error_message)
    sys.exit(1)

if __name__  == '__main__':
    read_config()
    metrics_list = get_metrics_list()
    print(metrics_list)
    for metric in metrics_list:
        print(metric)
        (status, json_response) = query_prometheus(nodeaddr,metric)
        if status:
            its = prom_query_to_elastic_items(json_response)
            for d_it in its:
                success = upload_elastic_item(d_it)
                if success == False:
                    print("[Prometheus Adapter]: Error uploading item: Item timestamp: " + str(d_it["timestamp"]) + " , Metric name: " + d_it["metric_name"])  
        else:
             print("[Prometheus Adapter]: Error querying prometheus: " + json_response)



