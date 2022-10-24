import json
import socket
import string
import csv
import sys

from nltk.tokenize import sent_tokenize, word_tokenize
from textwrap import wrap
from bs4 import BeautifulSoup

all_sentences = dict()
total_sentences = 0
words = dict()
total_words = 0


def is_isixhosa(sock, text, confidence=0.5):
    request = json.dumps({"text": text, "benchmark": 0}, ensure_ascii=False)
    sock.send(request.encode("utf-8"))
    response_text = sock.recv(4096).decode("utf-8").replace('confidence:"', 'confidence":')
    res = json.loads(response_text)
    return res is not None and res["language"] == "isiXhosa" and res["confidence"] > confidence


with open("isixhosa.jl") as f:
    lines = [f.readline()]

with socket.socket() as nchlt_sock:
    nchlt_sock.connect((socket.gethostname(), 7770))
    writer = csv.writer(sys.stdout)
    writer.writerow(["Total words", "Unique words"])

    for line in lines:
        record = json.loads(line)
        if "content" in record:
            # Parse the document and extract sentences for language ID
            soup = BeautifulSoup(record["content"], 'lxml')
            text = soup.get_text(" ", strip=True)
            sentences = [wrapped for sentence in sent_tokenize(text) for wrapped in wrap(sentence, width=300)]
            punct = set(string.punctuation)
            punct.add("“")

            for sentence in sentences:
                total_sentences += 1
                if is_isixhosa(nchlt_sock, sentence):
                    if sentence not in all_sentences:
                        all_sentences[sentence] = 0
                    all_sentences[sentence] += 1

                    print(sentence)

                    for word in word_tokenize(sentence):
                        word = word.lower()
                        # Skip only punctuation/digits
                        if all(c.isdigit() or c in punct for c in word):
                            continue

                        if word not in words:
                            words[word] = 0
                        words[word] += 1
                        total_words += 1

                        if total_words % 1000 == 0:
                            writer.writerow([total_words, len(words)])

                    continue
            sys.exit(0)

    writer.writerow([total_words, len(words)])

    writer.writerow([])
    writer.writerow(["Rank", "Word", "Frequency"])
    for (rank, (key, value)) in enumerate(sorted(words.items(), reverse=True, key=lambda x: x[1])[:1000]):
        writer.writerow([rank, key, value])

    writer.writerow([])
    writer.writerow(["Total sentences", "Unique sentences"])
    writer.writerow([total_sentences, len(all_sentences)])
