import json

with open('dev_data/dev.json', 'r') as infile:
    dev_data = json.load(infile)

with open('dev_data/dev_flat.jsonl', 'w') as outfile:
    for entry in dev_data:
        outfile.write(json.dumps(entry) + '\n')


