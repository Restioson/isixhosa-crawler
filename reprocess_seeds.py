from tldextract import tldextract
from crawler.spiders.isixhosa import blocked
f = open("seeds.txt")
seeds = set(f.readlines())  # Deduplicate
f.close()

f = open("seeds.processed.txt", "w")
blocked_domains = set()  # We do want to have each domain at least once, even if blocked


def get_domain(url):
    ext = tldextract.extract(url)
    return ".".join(ext[1:])


for seed in seeds:
    if not blocked(seed):
        f.write(seed)
    else:
        if get_domain(seed) not in blocked_domains:
            f.write(seed)
            blocked_domains.add(get_domain(seed))

f.close()
