#!/usr/bin/env python3

import requests
import json
import urllib3

urllib3.disable_warnings()


results = []
url = 'https://www.4byte.directory/api/v1/signatures/'
count = 0
while True:
    count += 1
    if count % 10 == 0:
        print(float(count)/5380)
    resp = requests.get(url, verify=False)
    result = json.loads(resp.text)
    results.extend(result['results'])
    url = result['next']
    if not url:
        break

with open('signatures.json', 'w') as f:
    json.dump(results, f, indent=2)
