import scrapy
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
import tldextract
from bs4 import BeautifulSoup, Comment
import socket
import json
from nltk.tokenize import sent_tokenize
from textwrap import wrap
import time

BLOCK_LIST = (
    # Known to contain lots of structured, downloadable isiXhosa - not interesting to crawl
    "wikipedia.org", "wikidata.org", "wikimedia.org", "wiktionary.org", "wikisource.org",

    # Navigation elements are translated, and/or content is machine translated
    "airbnb.co.za", "airbnb.com", "postermywall.com", "ubuy.co.zw", "ubuy.za.com",

    # Exist solely to provide (often machine translated) dictionaries and translations
    "opentran.net", "glosbe.com", "translated.net", "nativelib.net", "bab.la", "english-dictionary.help",
    "kasahorow.org",

    # Machine translated (overlaps slightly with GTranslate)
    "yatohandtools.com", "birmiss.com", "delachieve.com", "eferrit.com", "skopelos.com",
    "czechbrewerysystem.com", "manuals.plus", "czechminibreweries.com", "urdolls.com", "hotelleonor.sk",
    "atomiyme.com", "motio.com", "alsina-sa.com", "ubunlog.com", "bezzia.com", "photosforsouls.com",
    "xperimentalhamid.com", "madreshoy.com", "meteorologiaenred.com", "educationplanetonline.com",
    "solidsmack.com", "builttobrag.com", "descubrir.online", "hablemosdepeces.com", "lavamagazine.com",
    "medfem.co.za", "gm-chk.com", "gamingzeta.com", "ik4.es", "hombresconestilo.com", "cibercactus.com",
    "paradacreativa.es", "currentschoolnews.com", "yogaoutbreak.com", "cultureoeuvre.com",

    # GTranslate comment stripped
    "worldscholarshub.com", "wikiejemplos.com", "aspergillosis.org", "todoroblox.com", "vondt.net", "europeantimes.news",
    "media-sl.com", "xh.eturbonews.com",

    # Many, many duplicate pages (permalinks), and all the IsiXhosa is not machine readable (images/pdfs)
    "yumpu.com",
)


def blocked(url: str):
    ext = tldextract.extract(url)
    return ".".join(ext[1:]) in BLOCK_LIST


def identify(sock, text):
    text = json.dumps({"text": text, "benchmark": 0}, ensure_ascii=False)
    sock.send(text.encode("utf-8"))
    text = sock.recv(4096).decode("utf-8").replace('confidence:"', 'confidence":')
    return json.loads(text)


class IsiXhosaSpider(scrapy.Spider):
    name = "isixhosa"
    link_extractor = LinkExtractor()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        f = open("seeds.txt")
        self.start_urls = f.readlines()
        f.close()

        self.sock = socket.socket()
        self.sock.connect((socket.gethostname(), 7770))

    def start_requests(self):
        return [Request(url, callback=self.parse, priority=1000, dont_filter=False) for url in self.start_urls]

    def identify(self, text):
        text = json.dumps({"text": text, "benchmark": 0}, ensure_ascii=False)
        response_text = None
        response = None

        for _ in range(0, 3):
            try:
                self.sock.send(text.encode("utf-8"))
                response_text = self.sock.recv(4096).decode("utf-8").replace('confidence:"', 'confidence":')
                response = json.loads(response_text)
            except socket.error as err:
                print(f"err = '{err}'; text =  '{text}'")
                time.sleep(5)
                self.sock.close()
                self.sock = socket.socket()
                self.sock.connect((socket.gethostname(), 7770))
            except json.decoder.JSONDecodeError as err:
                print(f"err = '{err}'; text = '{text}'; response_text = '{response_text}'")
                break

        return response

    def is_isixhosa(self, text, confidence=0.5):
        res = self.identify(text)
        return res is not None and res["language"] == "isiXhosa" and res["confidence"] > confidence

    def parse(self, response, **kwargs):
        # Make sure to definitely crawl every seed URL. Then, set the URLs to [] so that it isn't done twice
        reqs = self.start_requests()
        self.start_urls = []
        for req in reqs:
            yield req

        # Parse the document and extract sentences for language ID
        soup = BeautifulSoup(response.text, 'lxml')
        text = soup.get_text(" ", strip=True)
        sentences = [wrapped for sentence in sent_tokenize(text) for wrapped in wrap(sentence, width=300)]

        # No machine-readable text
        if len(sentences) == 0:
            return

        # Identify sentences, specifically flagging isiXhosa and isiZulu
        # isiXhosa is what is most interesting, but isiZulu is also interesting as it is very similar to isiXhosa
        # It is expected that the language ID algorithm can distinguish between the two, but for testing and debugging,
        # it is useful to be able to see if it may be struggling to distinguish between them
        n_isixhosa, n_isizulu = 0, 0
        for sentence in sentences:
            ret = self.identify(sentence)

            if ret is None:
                print(f"Error on {response.url}")
                continue

            if ret["confidence"] < 0.5:
                continue

            if ret["language"] == "isiXhosa":
                n_isixhosa += 1
            elif ret["language"] == "isiZulu":
                n_isizulu += 1

        frac_isixhosa, frac_isizulu = n_isixhosa / len(sentences), n_isizulu / len(sentences)

        def log(action):
            print(f"{action} {response.url}. %/# isiXhosa: {frac_isixhosa * 100:.2f}/{n_isixhosa}. %/# isiZulu: {frac_isizulu * 100:.2f}/{n_isizulu}")

        crawl_all = False

        if n_isixhosa >= 5 or frac_isixhosa >= 0.4:
            log("Saving")
            crawl_all = True
            yield {"url": response.url, "frac_isixhosa": frac_isixhosa, "frac_isizulu": frac_isizulu, "content": response.text}

            # Skip page's links if it is machine translated. Specifically, GTranslate is checked since it is very common
            # We do save it, though, since it is useful to know what portion of domains are using GTranslate
            g_translate = any(soup.find_all(
                string=lambda elem: isinstance(elem, Comment) and elem.strip().startswith("delivered by GTranslate")
            ))
            if g_translate:
                log("Skipping (GTranslate)")
                return None

            # Also skip a page's links if it is in the blacklist
            if blocked(response.url):
                log("Skipping (Blacklisted)")
                return None
        elif n_isixhosa == 0:
            log("No isiXhosa here")
        else:
            crawl_all = True
            log("Continuing crawl")

        # Best effort link extraction - doesn't work for all sites (e.g. 13africanfarmers.com), as some use a form with
        # GET and query str
        for link in self.link_extractor.extract_links(response):
            url = response.urljoin(link.url)
            has_xhosa = "xhosa" in link.text.lower()
            is_isixhosa = self.is_isixhosa(link.text)
            priority = int(has_xhosa) + int(is_isixhosa)
            if not blocked(url) and (crawl_all or has_xhosa or is_isixhosa):
                yield {"from": response.url, "to": url}
                yield Request(url, callback=self.parse, priority=priority)
