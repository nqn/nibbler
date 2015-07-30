import json, time, requests, argparse, time

import nibbler

from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Nibbler collects statistics and metrics from a Mesos slave and push them to kafka')
    parser.add_argument('--slave', default='localhost:5051', type=str, help='hostname and port for mesos slave')
    parser.add_argument('--slave-state', default='state.json', type=str, help='path to state.json')
    parser.add_argument('--slave-metrics', default='metrics/snapshot', type=str, help='path to metrics snapshot json')
    parser.add_argument('--broker', required=True, type=str, help='location of kafka broker')

    args = parser.parse_args()

    slave_location = args.slave

    metrics_endpoint = 'http://%s/%s' % (slave_location, args.slave_metrics)
    slave_endpoint = 'http://%s/%s' % (slave_location, args.slave_state)

    client = KafkaClient(args.broker)
    producer = SimpleProducer(client)

    # One second sample rate.
    sample_rate = 1

    # Get slave state object once to tag samples.
    slave_state = nibbler.json_from_url(slave_endpoint)
    slave_id = slave_state['id']

    samples = {}
    sample_count = 0

    # Sample loop.
    while True:
        # Collect the latest metrics (gauges and counters).
        metrics = nibbler.json_from_url(metrics_endpoint)
        kafka_sample = {'slave_id': slave_id, 'timestamp': time.time(), 'metrics': metrics}
        producer.send_messages('nibbler_metrics', json.dumps(kafka_sample))

        time.sleep(sample_rate)

