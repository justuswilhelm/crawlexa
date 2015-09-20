from asyncio import (
    get_event_loop,
    Semaphore,
    Task,
    wait,
    wait_for,
)
from concurrent.futures._base import TimeoutError
import logging
import json
from urllib.parse import (
    urljoin,
    urlparse,
)
from re import compile

from redis import Redis
from aiohttp import get
from aiohttp.errors import (
    ClientOSError,
    ContentEncodingError,
)

MAX_DEPTH = 2
URL_RE = compile(r'(?i)href=["\']?([^\s"\'<>]+)')
URL_IGNORE_RE = compile(r'.*\.(png|css|ico|jpg)')

r = Redis()
logging.basicConfig(level=logging.DEBUG)
page_titles = {}
connection_sem = Semaphore(5)


async def get_urls(site_url, depth=0, loop=None):
    async def retrieve_site(url, timeout=1):
        logging.debug("%s: retrieving", url)
        await connection_sem.acquire()
        try:
            response = await wait_for(get(url), timeout)
        # Potential Errors:
        # ValueError: Host could not be detected
        # ...
            logging.debug("%s: Connected, retrieving text", url)
        except (ValueError, TimeoutError, ClientOSError):
            logging.debug("%s: Connection error", url)

        try:
            text = await wait_for(response.text(), timeout)
            logging.debug("%s: Retrieved", url)
        # Potential Errors:
        # ...
        except (UnicodeDecodeError, UnboundLocalError, TimeoutError):
            logging.debug("%s: Could not retrieve text", url)
            text = ''

        connection_sem.release()
        return text

    async def process_anchors(text):
        hrefs = URL_RE.findall(text)
        href_targets = set()
        for href in hrefs:
            if not href.startswith('http'):
                href = urljoin(site_url, href)
            # Remove potential query parameter from URL
            href, *_ = href.split('?')
            if not URL_IGNORE_RE.match(href) and depth < MAX_DEPTH:
                href_targets.add(href)
        for href_target in href_targets:
            if r.sismember('seen', href_target):
                continue
            logging.debug("Enqueueing %s", site_url)
            loop.create_task(get_urls(href_target, depth + 1, loop=loop))

    r.sadd('seen', site_url)
    logging.info("Crawling %s", site_url)

    if not r.exists(site_url):
        logging.debug("%s not in cache", site_url)
        text = await retrieve_site(site_url, timeout=5)

        # Only cache if text has been retrieved successfully
        if text:
            r.set(site_url, text)
            r.expire(site_url, 60 * 60 * 24)
    else:
        logging.debug("Retrieved %s from cache", site_url)
        text = r.get(site_url)

    await process_anchors(str(text))
    tasks = Task.all_tasks(loop)
    logging.info("Semaphore locked? %s", connection_sem.locked())
    logging.info("Remaining tasks: %d", len(tasks) - 1)
    if len(tasks) == 1:
        loop.stop()



def main():
    loop = get_event_loop()

    r.delete('seen')
    loop.create_task(get_urls('http://www.alexa.com/topsites', loop=loop))
    loop.run_forever()

    with open('results.json', 'w') as fd:
        fd.write(json.dumps(page_titles))


if __name__ == "__main__":
    main()
