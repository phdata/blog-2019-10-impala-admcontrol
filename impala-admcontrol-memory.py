#!/usr/bin/env python3
"""
This is a script put together to pull Impala query statistics and generate a
report to help guide the deployment of Impala Admission Control.
"""

__author__ = "phData, Inc"
__version__ = "1.0.0"
__license__ = "ASFv2"

import argparse
from configparser import ConfigParser
import json
import datetime
import copy
import os.path
import numpy as np
import requests
import urllib3
urllib3.disable_warnings()

def human_size(number):
    return round(number/1024/1024/1024, 2)

def human_time(number):
    return round(number/1000/60, 4)

def calc_average(number=[]):
    avg = 0
    if number:
        avg = sum(number)/len(number)
    return avg

def calc_percentile(number, percentile):
    perc = 0
    if number:
        npo = np.array(number)
        perc = np.percentile(npo, percentile)
    return perc

def max_safe(number):
    try:
        max_val = max(number)
    except ValueError:
        max_val = 0
    return max_val

def print_csv_report(memory_usage):
    # csv output
    fields = ['username',
              'queries_count',
              'queries_count_missing_stats',
              'aggregate_avg_gb',
              'aggregate_99th_gb',
              'aggregate_max_gb',
              'per_node_avg_gb',
              'per_node_99th_gb',
              'per_node_max_gb',
              'duration_avg_minutes',
              'duration_99th_minutes',
              'duration_max_minutes',]

    print(",".join(fields))

    for user in sorted(memory_usage):
        print('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % \
            (user,
             len(memory_usage[user]['stats_missing_false']['durationMillis']),
             len(memory_usage[user]['stats_missing_true']['durationMillis']),
             human_size(calc_average(memory_usage[user]['stats_missing_false']['memory_aggregate_peak'])),
             human_size(calc_percentile(memory_usage[user]['stats_missing_false']['memory_aggregate_peak'], 99)),
             human_size(max_safe(memory_usage[user]['stats_missing_false']['memory_aggregate_peak'])),
             human_size(calc_average(memory_usage[user]['stats_missing_false']['memory_per_node_peak'])),
             human_size(calc_percentile(memory_usage[user]['stats_missing_false']['memory_per_node_peak'], 99)),
             human_size(max_safe(memory_usage[user]['stats_missing_false']['memory_per_node_peak'])),
             human_time(calc_average(memory_usage[user]['stats_missing_false']['durationMillis'])),
             human_time(calc_percentile(memory_usage[user]['stats_missing_false']['durationMillis'], 99)),
             human_time(max_safe(memory_usage[user]['stats_missing_false']['durationMillis']))))

def main(cloudera_api, number_of_days):
    # set date range
    date_from = (datetime.datetime.now() + datetime.timedelta(-number_of_days)).strftime("%Y-%m-%d")
    date_to = datetime.datetime.now().strftime("%Y-%m-%d")

    # build our url string
    api_url = '%s://%s:%s/api/%s/clusters/%s/services/impala/impalaQueries?from=%s&to=%s&filter=&limit=1000' % \
            (cloudera_api['protocol'],
             cloudera_api['hostname'],
             cloudera_api['port'],
             cloudera_api['version'],
             cloudera_api['cluster'],
             date_from,
             date_to)

    # our data structure for each user
    schema = {}
    schema['stats_missing_true'] = {}
    schema['stats_missing_true']['memory_aggregate_peak'] = []
    schema['stats_missing_true']['memory_per_node_peak'] = []
    schema['stats_missing_true']['durationMillis'] = []
    schema['stats_missing_false'] = {}
    schema['stats_missing_false']['memory_aggregate_peak'] = []
    schema['stats_missing_false']['memory_per_node_peak'] = []
    schema['stats_missing_false']['durationMillis'] = []

    # tracking overall stats
    memory_usage = {}
    memory_usage['_ALL_'] = copy.deepcopy(schema)

    offset = 0
    while True:
        # looping and pulling 1,000 queries at a time
        tmp_url = api_url + '&offset=' + str(offset)
        offset += 1000
        response = requests.get(tmp_url, auth=(cloudera_api['username'], cloudera_api['password']), verify=False)
        if response.status_code != 200:
            exit(response.text)

        json_data = json.loads(response.text)
        if not json_data['queries']:
            # we have ran out of pages to go through
            break

        # loop through our queries
        for query in json_data['queries']:
            # use any queries with memory stats
            if 'memory_aggregate_peak' in query['attributes']:
                username = query['user']
                memory_aggregate_peak = float(query['attributes']['memory_aggregate_peak'])
                memory_per_node_peak = float(query['attributes']['memory_per_node_peak'])
                duration = query['durationMillis']

                # make sure we have structure for each user
                if username not in memory_usage:
                    memory_usage[username] = copy.deepcopy(schema)

                # we want to measure with and without stats
                category = 'stats_missing_' + query['attributes']['stats_missing']

                for tmp_user in ['_ALL_', username]:
                    memory_usage[tmp_user][category]['memory_aggregate_peak'].append(memory_aggregate_peak)
                    memory_usage[tmp_user][category]['memory_per_node_peak'].append(memory_per_node_peak)
                    memory_usage[tmp_user][category]['durationMillis'].append(duration)

    # print out a CSV of our data
    print_csv_report(memory_usage)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", action="store", dest="config",
                        required=True, help="path to your configuration file")
    parser.add_argument("-d", "--days", action="store", dest="number_of_days",
                        required=True, help="number of days to query")
    args = parser.parse_args()

    # Does the config file actually exist?
    if os.path.exists(args.config) is False:
        exit('invalid config file')

    # Create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(args.config)

    # Get config section
    cloudera_api = {}
    items = parser.items('config')
    for item in items:
        cloudera_api[item[0]] = item[1]

    main(cloudera_api, int(args.number_of_days))
