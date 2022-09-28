import json
import sys

import tldextract

sites = dict()

if __name__ == "__main__":
    f = open("out/xhosa.jl")
    lines = f.readlines()
    f.close()

    if sys.argv[-1] == "since_last":
        last_file = open("out/last_index.txt", "r")
        last = int(last_file.read())
        last_file.close()

        last_file = open("out/last_index.txt", "w")
        last_file.write(str(len(lines)))
        last_file.close()

        lines = lines[last:]

    for line in lines:
        record = json.loads(line)
        if "url" not in record:
            continue
        domain = ".".join(s for s in tldextract.extract(record["url"]) if s)
        if sys.argv[-1] == "since_last":
            print(record["url"])
        if domain not in sites:
            sites[domain] = 0
        sites[domain] += 1

    for key, value in sorted(sites.items(), reverse=True, key=lambda x: x[1]):
        print("{} : {}".format(key, value))

docs = 0
for v in sites.values():
    docs += v
print(docs)
