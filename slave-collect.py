import os, sys, json, math, urllib, time, requests, argparse

def push(slave_id, framework_id, executor_id, obj):
	out = []
	for id_, item in obj.iteritems():
		if isinstance(item, dict):
			out.extend(push(slave_id, framework_id, executor_id, item))
		else:
			out.append({
					"name": id_,
					"columns": [ "value", "slave_id", "framework_id", "executor_id"],
					"points": [ [ item, slave_id, framework_id, executor_id] ]
			})

	return out

def parse(url):	
	response = urllib.urlopen(url);
	data = json.loads(response.read())
	return data

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description='Nibbler collects statistics and metrics from a Mesos slave and push them to influxdb')
	parser.add_argument('--slave', default='localhost:5051', type=str, help='hostname and port for mesos slave')
	parser.add_argument('--influxdb-host', default='localhost:8086', type=str, help='hostname and port for influxdb admin server')
	parser.add_argument('--influxdb-name', required=True, type=str, help='Database name to use')
	parser.add_argument('--influxdb-user', default='root', type=str, help='user for influxdb admin server')
	parser.add_argument('--influxdb-password', default='root', type=str, help='password for influxdb admin server')

	args = parser.parse_args()

	slave_location = args.slave

	monitor_endpoint = 'http://%s/monitor/statistics.json' % slave_location
	metrics_endpoint = 'http://%s/metrics/snapshot' % slave_location
	slave_endpoint = 'http://%s/state.json' % slave_location
	influx_endpoint = 'http://%s/db/%s/series?u=%s&p=%s' % (args.influxdb_host, args.influxdb_name, args.influxdb_user, args.influxdb_password)

	create_json = '{"name": "%s"}' % args.influxdb_name
	create_url = 'http://%s/db?u=%s&p=%s' % (args.influxdb_host, args.influxdb_user, args.influxdb_password)
	requests.post(url=create_url, data=create_json, headers={'Content-Type': 'application/octet-stream'})

	# One second sample rate.
	sample_rate = 1

	# Get slave state object once to tag samples.
	slave_state = parse(slave_endpoint)
	slave_id = slave_state['id']

	samples = {}

	while True:
		# Poor mans GC: We loose one sample per framework every 10.000 iterations.
		sample_count += 1
		if sample_count > 10000 == 0:
			sample_count = 0
			samples = {}

		influx_samples = []

		# Collect the latest resource usage statistics.
		for sample in parse(monitor_endpoint):
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
				system_time = sample['statistics']['cpus_system_time_secs'] - prev['statistics']['cpus_system_time_secs']
				cpu_usage = (user_time + system_time) / interval

				influx_samples.append({
						"name": "cpu_usage",
						"columns": [ "value", "slave_id", "framework_id", "executor_id"],
						"points": [ [ cpu_usage, slave_id, framework_id, executor_id] ]
				})

				influx_samples.extend(push(slave_id, framework_id, executor_id, sample['statistics']))

				# Compute slack CPU.
				cpu_slack = sample['statistics']['cpus_limit'] - cpu_usage
				influx_samples.append({
				    "name": "cpu_slack",
				    "columns": [ "value", "slave_id", "framework_id", "executor_id"],
				    "points": [ [ cpu_slack, slave_id, framework_id, executor_id] ]
				})

				# Compute slack memory.
				mem_slack = sample['statistics']['mem_limit_bytes'] - sample['statistics']['mem_rss_bytes']
				influx_samples.append({
						"name": "mem_slack",
						"columns": [ "value", "slave_id", "framework_id", "executor_id"],
						"points": [ [ mem_slack, slave_id, framework_id, executor_id] ]
				})

			samples[framework_id][executor_id] = sample

		# Collect the latest metrics (gauges and counters).
		metrics = parse(metrics_endpoint)
		for metric in metrics:
			influx_samples.append({
					"name": metric,
					"columns": [ "value", "slave_id", "source"],
					"points": [ [ metrics[metric], slave_id, "slave"] ]
			})


		# Send samples if collected.
		if influx_samples is not '':
			json_out = json.dumps(influx_samples)
			print requests.post(url=influx_endpoint, data=json_out, headers={'Content-Type': 'application/octet-stream'})
			print "Sent sample..."

		time.sleep(sample_rate)
