import json
from crawler.spiders.isixhosa import blocked

if __name__ == "__main__":
    in_file = open("out/isixhosa.jl")
    lines = in_file.readlines()
    in_file.close()

    out_file = open("out/isixhosa.new.jl", "w")

    for line in lines:
        record = json.loads(line)
        allowed_link = "url" not in record and not (blocked(record["to"]) or blocked(record["from"]))
        allowed_page = "url" in record and not blocked(record["url"])
        if allowed_link or allowed_page:
            out_file.write(line)

    out_file.close()
