import argparse
import time
import json

import nibbler

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Nibbler collects statistics and metrics from a Mesos master and push them to influxdb')
    parser.add_argument('--master', default='localhost:5050', type=str, help='hostname and port for mesos master')
    parser.add_argument('--master-state', default='state.json', type=str, help='path to state.json')
    parser.add_argument('--master-metrics', default='metrics/snapshot', type=str, help='path to metrics snapshot json')
    parser.add_argument('--influxdb-host', default='localhost:8086', type=str,
                        help='hostname and port for influxdb admin server')
    parser.add_argument('--influxdb-name', required=True, type=str, help='Database name to use')
    parser.add_argument('--influxdb-user', default='root', type=str, help='user for influxdb admin server')
    parser.add_argument('--influxdb-password', default='root', type=str, help='password for influxdb admin server')

    args = parser.parse_args()

    master_location = args.master

    metrics_endpoint = 'http://%s/%s' % (master_location, args.master_metrics)
    master_endpoint = 'http://%s/%s' % (master_location, args.master_state)
    influx_endpoint = 'http://%s/db/%s/series?u=%s&p=%s' % (
        args.influxdb_host, args.influxdb_name, args.influxdb_user, args.influxdb_password)

    create_json = '{"name": "%s"}' % args.influxdb_name
    create_url = 'http://%s/db?u=%s&p=%s' % (args.influxdb_host, args.influxdb_user, args.influxdb_password)
    nibbler.post_json(create_url, create_json)

    # One second sample rate.
    sample_rate = 1

    # Sample loop.
    while True:
        # TODO(nnielsen): Don't fetch state json every iteration.
        # Get slave state object once to tag samples and determine leader.
        master_state = nibbler.json_from_url(master_endpoint)

        if "leader" in master_state and "pid" in master_state:
            if master_state["leader"] != master_state["pid"]:
                print "Master is not the current leader - waiting 5 seconds"
                time.sleep(5)
                continue

        master_id = master_state["id"]

        influx_samples = []

        # Collect the latest metrics (gauges and counters).
        metrics = nibbler.json_from_url(metrics_endpoint)
        for metric in metrics:
            influx_samples.append({
                "name": metric,
                "columns": ["value", "master_id", "source"],
                "points": [[metrics[metric], master_id, "master"]]
            })

        # Send samples if collected.
        if influx_samples is not '':
            json_out = json.dumps(influx_samples)
            print nibbler.post_json(influx_endpoint, json_out)
            print "Sent sample..."

        time.sleep(sample_rate)