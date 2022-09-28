import csv

from crawler.spiders.isixhosa import blocked, identify
import requests
import socket
import random

with open('google_secrets.txt') as f:
    lines = f.readlines()
    key, engine_id = lines[0], lines[1]

words = set()
with open('words.csv') as csv_file:
    rdr = csv.reader(csv_file, delimiter=',', quotechar='"')
    for row in rdr:
        words.add(row[2])

words = list(words)
random.shuffle(words)

sock = socket.socket()
sock.connect((socket.gethostname(), 7770))

urls = set()

# Only limit of 100 so need to exit early sometimes
try:
    for _ in range(0, 100):
        query = words.pop()
        url = f"https://www.googleapis.com/customsearch/v1?key={key}&cx={engine_id}&q={query}"
        results = requests.get(url).json()

        if "items" not in results:
            print(f"No results for {query}")
            continue
        else:
            print(f"{results['searchInformation']['totalResults']} results for {query}")

        for result in results["items"]:
            url = result["link"]

            if "snippet" not in result:
                continue

            is_xhosa = identify(sock, result["snippet"])["language"] == "isiXhosa"
            crawlable_document = not (url.endswith("pdf") or url.endswith("xlsx") or url.endswith("docx"))
            if is_xhosa and not blocked(url) and crawlable_document:
                urls.add(url)

        print(f"{len(urls)} in total")
except:
    pass

f = open("seeds2.txt", "w")
f.write("\n".join(urls))
f.close()
