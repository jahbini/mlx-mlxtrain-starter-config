# pip install requests beautifulsoup4 html2text
import requests, json, re, time, random
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urldefrag

BASE = "https://stjohnsjim.com/"
START_URL = BASE
SCHEMA = "instruct"        # or "chat"
VALID_FRACTION = 0.2
USER_AGENT = "SJJ-JSONL/2.0 (+crawler for fine-tune prep)"
SEED = 42
REQUEST_TIMEOUT = 20
PAUSE_SEC = 0.6
MIN_WORDS = 80             # skip super-short pages

# optional: use html2text if present, else use a lightweight cleaner
try:
    import html2text
    mdify = html2text.HTML2Text()
    mdify.ignore_links = True
    mdify.ignore_images = True
    mdify.body_width = 0
    def html_to_text(html: str) -> str:
        return mdify.handle(html)
except Exception:
    def html_to_text(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for t in soup(["script","style","noscript","nav","footer"]):
            t.decompose()
        for br in soup.find_all("br"):
            br.replace_with("\n")
        # headings -> prefixed text
        for h in soup.find_all(["h1","h2","h3","h4","h5","h6"]):
            level = int(h.name[1])
            h.string = f"{'#'*level} {h.get_text(strip=True)}"
        # lists
        for ul in soup.find_all("ul"):
            items = ["- " + li.get_text(" ", strip=True) for li in ul.find_all("li")]
            ul.replace_with("\n".join(items))
        for ol in soup.find_all("ol"):
            items = [f"{i}. {li.get_text(' ', strip=True)}" for i, li in enumerate(ol.find_all("li"), 1)]
            ol.replace_with("\n".join(items))
        chunks = []
        for tag in soup.find_all(["h1","h2","h3","h4","h5","h6","p","article","section","div"]):
            txt = tag.get_text("\n", strip=True)
            if txt and len(txt.split()) > 3:
                chunks.append(txt)
        text = "\n\n".join(chunks)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text

def get(url):
    r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.text

def is_local_html(href: str) -> bool:
    # keep only same-domain (or relative) links ending with .html
    href, _ = urldefrag(href)
    if not href or not href.endswith(".html"):
        return False
    full = urljoin(BASE, href)
    u = urlparse(full)
    return (u.netloc == urlparse(BASE).netloc)

def discover_all_html(start_url: str):
    to_visit = [start_url]
    visited = set()
    pages = []
    while to_visit:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            html = get(url)
        except Exception:
            continue
        pages.append((url, html))
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if is_local_html(href):
                nxt = urljoin(BASE, href)
                if nxt not in visited and nxt not in to_visit:
                    to_visit.append(nxt)
        time.sleep(PAUSE_SEC)
    return pages

def parse_story(html: str, url: str):
    soup = BeautifulSoup(html, "html.parser")

    # Title: first <h2> (story heading) or <title> as fallback
    h2 = soup.find("h2")
    title = h2.get_text(strip=True) if h2 else (soup.title.get_text(strip=True) if soup.title else "Untitled")

    # Extract only the div#bloviation
    story_div = soup.find(id="bloviation")
    if not story_div:
        return None  # skip pages without story content

    # Turn it into plain/markdown-ish text
    text_md = html_to_text(str(story_div))
    text_md = re.sub(r"\n{3,}", "\n\n", text_md).strip()

    slug = urlparse(url).path.rsplit("/",1)[-1].replace(".html","")

    return {
        "doc_id": slug,
        "title": title,
        "text": text_md,
        "url": url
    }

def make_example(rec):
    if SCHEMA == "chat":
        return {
            "meta": {"doc_id": rec["doc_id"], "title": rec["title"], "url": rec["url"]},
            "messages": [
                {"role": "system", "content": "You are a helpful, concise assistant who writes in the author's voice when asked."},
                {"role": "user", "content": f"Summarize this story in 5–8 sentences, preserving tone and key imagery:\n\nTitle: {rec['title']}\n\n{rec['text']}"},
                {"role": "assistant", "content": "<<<PUT_YOUR_GOLD_SUMMARY_OR_TEACHER_OUTPUT_HERE>>>"}
            ]
        }
    else:  # instruct (default)
        return {
            "meta": {"doc_id": rec["doc_id"], "title": rec["title"], "url": rec["url"]},
            "instruction": "Summarize the following story in 5–8 sentences, preserving tone and key imagery.",
            "input": f"Title: {rec['title']}\n\n{rec['text']}",
            "output": "<<<PUT_YOUR_GOLD_SUMMARY_OR_TEACHER_OUTPUT_HERE>>>"
        }

def main():
    print("Crawling… (site-local .html only)")
    pages = discover_all_html(START_URL)
    print(f"Fetched {len(pages)} HTML pages; parsing…")

    examples = []
    for url, html in pages:
        try:
            rec = parse_story(html, url)
            if len(rec["text"].split()) < MIN_WORDS:
                continue
            examples.append(make_example(rec))
        except Exception:
            pass

    # de-dup by doc_id (last one wins)
    uniq = {}
    for ex in examples:
        uniq[ex["meta"]["doc_id"]] = ex
    examples = list(uniq.values())

    random.seed(SEED)
    random.shuffle(examples)

    n_valid = max(1, int(len(examples) * VALID_FRACTION))
    valid = examples[:n_valid]
    train = examples[n_valid:]

    with open("train.jsonl","w",encoding="utf-8") as f:
        for ex in train:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")
    with open("valid.jsonl","w",encoding="utf-8") as f:
        for ex in valid:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")

    print(f"Wrote train.jsonl ({len(train)}) and valid.jsonl ({len(valid)}) using schema='{SCHEMA}'.")

if __name__ == "__main__":
    main()
