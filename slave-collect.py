import json, time, requests, argparse, time

import nibbler

def validate_statistics_sample(sample):
    if 'framework_id' not in sample:
        print 'Framework ID not found in sample'
        return False

    if 'executor_id' not in sample:
        print 'Executor ID not found in sample'
        return False

    if 'statistics' not in sample:
        print 'statistics not found in sample'
        return False

    if 'timestamp' not in sample['statistics']:
        print 'timestamp not found in sample'
        return False

    if 'cpus_user_time_secs' not in sample['statistics']:
        print 'cpu user time not found in sample'
        return False

    if 'cpus_system_time_secs' not in sample['statistics']:
        print 'cpu system time not found in sample'
        return False

    return True


def executor_metric(influx_samples, metric, value, slave_id, framework_id, executor_id):
    influx_samples.append({
        "name": metric,
        "columns": ["value", "slave_id", "framework_id", "executor_id"],
        "points": [[value, slave_id, framework_id, executor_id]]
    })

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Nibbler collects statistics and metrics from a Mesos slave and push them to influxdb')
    parser.add_argument('--slave', default='localhost:5051', type=str, help='hostname and port for mesos slave')
    parser.add_argument('--slave-state', default='state.json', type=str, help='path to state.json')
    parser.add_argument('--slave-monitor', default='monitor/statistics.json', type=str, help='path to statistics.json')
    parser.add_argument('--slave-metrics', default='metrics/snapshot', type=str, help='path to metrics snapshot json')
    parser.add_argument('--influxdb-host', default='localhost:8086', type=str,
                        help='hostname and port for influxdb admin server')
    parser.add_argument('--influxdb-name', required=True, type=str, help='Database name to use')
    parser.add_argument('--influxdb-user', default='root', type=str, help='user for influxdb admin server')
    parser.add_argument('--influxdb-password', default='root', type=str, help='password for influxdb admin server')

    args = parser.parse_args()

    slave_location = args.slave

    monitor_endpoint = 'http://%s/%s' % (slave_location, args.slave_monitor)
    metrics_endpoint = 'http://%s/%s' % (slave_location, args.slave_metrics)
    slave_endpoint = 'http://%s/%s' % (slave_location, args.slave_state)
    influx_endpoint = 'http://%s/db/%s/series?u=%s&p=%s' % (
        args.influxdb_host, args.influxdb_name, args.influxdb_user, args.influxdb_password)

    # One second sample rate.
    sample_rate = 1

    # Get slave state object once to tag samples.
    slave_state = nibbler.json_from_url(slave_endpoint)
    slave_id = slave_state['id']

    samples = {}
    sample_count = 0

    # Sample loop.
    while True:
        # If InfluxDB failed over, make sure database is available.
        if sample_count % 10 == 0:
            create_json = '{"name": "%s"}' % args.influxdb_name
            create_url = 'http://%s/db?u=%s&p=%s' % (args.influxdb_host, args.influxdb_user, args.influxdb_password)
            nibbler.post_json(create_url, create_json)

        # Poor mans GC: We loose one sample per framework every 10.000 iterations.
        sample_count += 1
        if sample_count > 10000 == 0:
            print "Cleaning samples..."
            sample_count = 0
            samples = {}

        influx_samples = []

        # Collect the latest resource usage statistics.
        for sample in nibbler.json_from_url(monitor_endpoint):
            if 'statistics' in sample and 'timestamp' not in sample['statistics']:
                sample['statistics']['timestamp'] = time.time()

            # Validate sample
            if validate_statistics_sample(sample) == False:
                print "Warning: partial sample %s" % sample
                continue

            framework_id = sample['framework_id']
            executor_id = sample['executor_id']

            if framework_id not in samples:
                samples[framework_id] = {}

            if executor_id not in samples[framework_id]:
                samples[framework_id][executor_id] = None

            if samples[framework_id][executor_id] is not None:
                # We need two samples to compute the cpu usage.
                prev = samples[framework_id][executor_id]

                interval = sample['statistics']['timestamp'] - prev['statistics']['timestamp']

                user_time = sample['statistics']['cpus_user_time_secs'] - prev['statistics']['cpus_user_time_secs']
                system_time = sample['statistics']['cpus_system_time_secs'] - prev['statistics'][
                    'cpus_system_time_secs']
                cpu_usage = (user_time + system_time) / interval

                influx_samples.append({
                    "name": "cpu_usage",
                    "columns": ["value", "slave_id", "framework_id", "executor_id"],
                    "points": [[cpu_usage, slave_id, framework_id, executor_id]]
                })

                influx_samples.extend(nibbler.influxdb_entries_from_object(slave_id, framework_id, executor_id, sample['statistics']))

                # Compute slack CPU.
                cpu_slack = sample['statistics']['cpus_limit'] - cpu_usage
		executor_metric(influx_samples, "cpu_slack", cpu_slack, slave_id, framework_id, executor_id)

                # Compute slack memory.
                mem_slack = sample['statistics']['mem_limit_bytes'] - sample['statistics']['mem_rss_bytes']
		executor_metric(influx_samples, "mem_slack", mem_slack, slave_id, framework_id, executor_id)

                # Compute IPC.
                if 'perf' in sample['statistics'] and 'perf' in prev['statistics']:
                    prev_perf = prev['statistics']['perf']
                    perf = sample['statistics']['perf']

                if (perf['timestamp'] > prev_perf['timestamp']) and ('cycles' in perf and 'instructions' in perf):
                    cycles = perf['cycles']
                    instructions = perf['instructions']

                    ipc = float(instructions) / float(cycles)
                    cpi = float(cycles) / float(instructions)
                    ips = float(instructions) / float(perf['duration'])

                    executor_metric(influx_samples, "ipc", ipc, slave_id, framework_id, executor_id)
                    executor_metric(influx_samples, "cpi", cpi, slave_id, framework_id, executor_id)
                    executor_metric(influx_samples, "ips", ips, slave_id, framework_id, executor_id)

            samples[framework_id][executor_id] = sample

        # Collect the latest metrics (gauges and counters).
        metrics = nibbler.json_from_url(metrics_endpoint)
        for metric in metrics:
            influx_samples.append({
                "name": metric,
                "columns": ["value", "slave_id", "source"],
                "points": [[metrics[metric], slave_id, "slave"]]
            })

        # Send samples if collected.
        if influx_samples is not '':
            json_out = json.dumps(influx_samples)
            print nibbler.post_json(influx_endpoint, json_out)
            print "Sent sample..."

        time.sleep(sample_rate)
