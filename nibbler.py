import requests, time, urllib, json


def influxdb_entries_from_object(slave_id, framework_id, executor_id, obj):
    out = []
    for id_, item in obj.iteritems():
        if isinstance(item, dict):
            out.extend(influxdb_entries_from_object(slave_id, framework_id, executor_id, item))
        else:
            out.append({
                "name": id_,
                "columns": ["value", "slave_id", "framework_id", "executor_id"],
                "points": [[item, slave_id, framework_id, executor_id]]
            })

    return out


def json_from_url(url):
    while True:
        try:
            response = urllib.urlopen(url)
            data = response.read()
            return json.loads(data)
        except IOError:
            print "Could not load %s: retrying in one second" % url
            time.sleep(1)
            continue


def post_json(url, data):
    while True:
        try:
            return requests.post(url=url, data=data, headers={'Content-Type': 'application/octet-stream'})
        except requests.exceptions.ConnectionError:
            print "Could not post to %s: retrying in one second" % url
            time.sleep(1)
            continue
