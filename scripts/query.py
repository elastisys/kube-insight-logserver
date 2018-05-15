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

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'starttime', metavar="<starttime>",
        help="start time: YYYY-MM-ddTHH:MM:SS")
    parser.add_argument(
        'endtime', metavar="<endtime>",
        help="end time: YYYY-MM-ddTHH:MM:SS")
    parser.add_argument(
        '--namespace', metavar="NAME", type=str, default="default",
        help="pod namespace. default: 'default'")
    parser.add_argument(
        '--pod-name', metavar="NAME", type=str, default="nginx-deployment-abcde",
        help="pod name. default: 'nginx-deployment-abcde'")
    parser.add_argument(
        '--container-name', metavar="NAME", default="nginx",
        help="container name. default: ngnix")
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

    log.info("executing query ...")
    params = urllib.parse.urlencode({
        'namespace': args.namespace,
        'pod_name': args.pod_name,
        'container_name': args.container_name,
        'start_time': starttime.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        'end_time': endtime.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    })
    conn = http.client.HTTPConnection(args.host, args.port)
    conn.request("GET", "/query?" + params)
    response = conn.getresponse()
    if response.status != 200:
        log.error("query failed: %s", json.dumps(json.loads(response.read()), indent=4))
    else:
        result = json.loads(response.read())
        log_rows = result["log_rows"]
        log.error("query returned %d rows: %s",
                  len(log_rows), json.dumps(log_rows, indent=4))
