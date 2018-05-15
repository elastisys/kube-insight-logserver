#!/usr/bin/env python3

import argparse
import http.client
import json
import logging
import urllib.parse
from datetime import datetime, timedelta
from string import Template

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s | %(message)s')
log = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = 100

def log_row(timestamp):
    t_seconds = (timestamp - datetime.utcfromtimestamp(0)).total_seconds()
    t_iso = timestamp.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    return   {
        "date": t_seconds,
        "kubernetes": {
            "docker_id": "e4b0b3eb8c25a73351c5cfeb37a9d64736584c640f21010443fe2e7e5b9c085b",
            "labels": {
                "app": "nginx",
                "pod-template-generation": "1"
            },
            "pod_id": "1021f36b-4e9e-11e8-8b6b-02425d6e035a",
            "host": "worker0",
            "pod_name": "nginx-deployment-abcde",
            "container_name": "nginx",
            "namespace_name": "default"
        },
        "log": '10.46.0.0 - - [' + t_iso + '] "GET /index.html HTTP/1.1" 200 647 "-" "kube-probe/1.10" "-"',
        "stream": "stdout",
        "time": t_iso
    }

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'starttime', metavar="<starttime>",
        help="start time: YYYY-MM-ddTHH:MM:SS")
    parser.add_argument(
        'endtime', metavar="<endtime>",
        help="end time: YYYY-MM-ddTHH:MM:SS")
    parser.add_argument(
        '--interval', metavar="SECONDS", type=int, default=10,
        help="batch log submit interval (seconds). default: 10")
    parser.add_argument(
        '--batch-size', metavar="ROWS", type=int, default=DEFAULT_BATCH_SIZE,
        help="the number of log rows to submit with every post. "
        "default: {}".format(DEFAULT_BATCH_SIZE))
    parser.add_argument(
        '--host', metavar="HOST", default="localhost",
        help="log server host. default: localhost")
    parser.add_argument(
        '--port', metavar="PORT", type=int, default=8080,
        help="log server port. default: 8080")

    args = parser.parse_args()
    log.debug(args)

    starttime = datetime.strptime(args.starttime, '%Y-%m-%dT%H:%M:%S').replace(microsecond=0)
    endtime = datetime.strptime(args.endtime, '%Y-%m-%dT%H:%M:%S').replace(microsecond=0)
    log.info("start time: %s", starttime.strftime("%Y-%m-%dT%H:%M:%S"))
    log.info("end time: %s", endtime.strftime("%Y-%m-%dT%H:%M:%S"))

    log.info("checking if server is up ...")
    conn = http.client.HTTPConnection(args.host, args.port)
    try:
        conn.request("GET", "/write")
        response = conn.getresponse()
        if response.status == 200:
            log.info("server is up")
        else:
            log.error("server is down: %s", response.read())
    finally:
        conn.close()

    t = starttime
    while t < endtime:
        # post batch of size <batch_size> with entries from t to t+<interval>
        log_entry_spacing_sec = float(args.interval) / args.batch_size
        batch = []
        for log_item in range(args.batch_size):
            batch_offset_sec = log_item * log_entry_spacing_sec
            log_time = t + timedelta(seconds=batch_offset_sec)
            batch.append(log_row(log_time))
        # submit
        #log.info("batch: %s", batch_json)
        t += timedelta(seconds=args.interval)
        log.info("sending batch of size %d at time %s",
                 len(batch), t.strftime("%H:%M:%S.%s"))
        batch_json = json.dumps(batch, indent=4)

        conn = http.client.HTTPConnection(args.host, args.port)
        try:
            conn.request("POST", "/write", body=batch_json,
                         headers={'Content-Type': 'application/json'})
            response = conn.getresponse()
            log.info("response: {}: {}".format(
                response.status, response.read()))
        finally:
            conn.close()
