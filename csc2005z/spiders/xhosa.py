import scrapy
from scrapy import Request
from scrapy.linkextractors import LinkExtractor
import tldextract
from bs4 import BeautifulSoup, Comment
import socket
import json
from nltk.tokenize import sent_tokenize
from textwrap import wrap

BLOCK_LIST = (
    # Known to contain lots of structured, downloadable Xhosa - not interesting to crawl
    "wikipedia.org", "wikidata.org", "wikimedia.org", "wiktionary.org", "wikisource.org",

    # Navigation elements are translated, and/or content is machine translated
    "airbnb.co.za", "airbnb.com", "postermywall.com",

    # Exist solely to provide (often machine translated) dictionaries and translations
    "opentran.net", "glosbe.com", "translated.net", "nativelib.net", "bab.la",

    # Machine translated (overlaps slightly with GTranslate)
    "yatohandtools.com", "birmiss.com", "delachieve.com", "eferrit.com", "skopelos.com",
    "czechbrewerysystem.com", "manuals.plus", "czechminibreweries.com", "urdolls.com", "hotelleonor.sk",
    "atomiyme.com", "motio.com", "alsina-sa.com", "ubunlog.com", "bezzia.com", "photosforsouls.com",
    "xperimentalhamid.com", "madreshoy.com", "meteorologiaenred.com", "educationplanetonline.com",
    "solidsmack.com", "builttobrag.com", "descubrir.online", "hablemosdepeces.com", "lavamagazine.com",
    "medfem.co.za", "gm-chk.com", "gamingzeta.com", "ik4.es", "hombresconestilo.com", "cibercactus.com",
    "paradacreativa.es", "currentschoolnews.com",

    # GTranslate comment stripped
    "worldscholarshub.com", "wikiejemplos.com", "aspergillosis.org",
)


def blocked(url: str):
    ext = tldextract.extract(url)
    return ".".join(ext[1:]) in BLOCK_LIST


def identify(sock, text):
    text = json.dumps({"text": text, "benchmark": 0}, ensure_ascii=False)
    response_text = None
    response = None

    for time in range(0, 3):
        try:
            sock.send(text.encode("utf-8"))
            response_text = sock.recv(4096).decode("utf-8").replace('confidence:"', 'confidence":')
            response = json.loads(response_text)
        except socket.error as err:
            sock = socket.socket()
            sock.connect((socket.gethostname(), 7770))
            print(f"err = '{err}'; text =  '{text}'")
        except json.decoder.JSONDecodeError as err:
            print(f"err = '{err}'; text = '{text}'; response_text = '{response_text}'")
            break

    return response


class XhosaSpider(scrapy.Spider):
    name = "xhosa"
    link_extractor = LinkExtractor()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        f = open("seeds.txt")
        self.start_urls = f.readlines()
        f.close()
        self.sock = socket.socket()
        self.sock.connect((socket.gethostname(), 7770))

    def start_requests(self):
        return (Request(url, callback=self.parse, priority=1000) for url in self.start_urls)

    def identify(self, text):
        return identify(self.sock, text)

    def is_xhosa(self, text, confidence=0.5):
        res = self.identify(text)
        return res is not None and res["language"] == "isiXhosa" and res["confidence"] > confidence

    def parse(self, response, **kwargs):
        # Parse the document and extract sentences for language ID
        soup = BeautifulSoup(response.text, 'lxml')
        text = soup.get_text(" ", strip=True)
        sentences = [wrapped for sentence in sent_tokenize(text) for wrapped in wrap(sentence, width=500)]

        # No machine-readable text
        if len(sentences) == 0:
            return

        # Identify sentences, specifically flagging isiXhosa and isiZulu
        # isiXhosa is what is most interesting, but isiZulu is also interesting as it is very similar to isiXhosa
        # It is expected that the language ID algorithm can distinguish between the two, but for testing and debugging,
        # it is useful to be able to see if it may be struggling to distinguish between them
        n_xhosa, n_zulu = 0, 0
        for sentence in sentences:
            ret = self.identify(sentence)

            if ret is None:
                print(f"Error on {response.url}")
                continue

            if ret["confidence"] < 0.5:
                continue

            if ret["language"] == "isiXhosa":
                n_xhosa += 1
            elif ret["language"] == "isiZulu":
                n_zulu += 1

        frac_xhosa, frac_zulu = n_xhosa / len(sentences), n_zulu / len(sentences)

        def log(action):
            print(f"{action} {response.url}. %/# Xhosa: {frac_xhosa * 100:.2f}/{n_xhosa}. %/# Zulu: {frac_zulu * 100:.2f}/{n_zulu}")

        # Skip page if it is machine translated. Specifically, GTranslate is checked since it is very common
        g_translate = any(soup.find_all(
            string=lambda elem: isinstance(elem, Comment) and elem.strip().startswith("delivered by GTranslate")
        ))
        if g_translate:
            log("Skipping (GTranslate)")
            return None

        # Also skip a page if it is in the bla
        if blocked(response.url):
            log("Skipping (Blacklisted)")
            return None

        crawl_all = False

        if n_xhosa >= 5 or frac_xhosa >= 0.4:
            log("Saving")
            crawl_all = True
            yield {"url": response.url, "frac_xhosa": frac_xhosa, "frac_zulu": frac_zulu, "content": response.text}
        elif n_xhosa == 0:
            log("No xhosa here")
        else:
            crawl_all = True
            log("Continuing crawl")

        # Best effort link extraction - doesn't work for all sites (e.g. 13africanfarmers.com), as some use a form with
        # GET and query str
        for link in self.link_extractor.extract_links(response):
            url = response.urljoin(link.url)
            has_xhosa = "xhosa" in link.text.lower()
            is_xhosa = self.is_xhosa(link.text)
            priority = int(has_xhosa) + int(is_xhosa)
            if not blocked(url) and (crawl_all or has_xhosa or is_xhosa):
                yield {"from": response.url, "to": url}
                yield Request(url, callback=self.parse, priority=priority)
