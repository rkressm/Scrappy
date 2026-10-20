import asyncio
import aiohttp
from helpers import normalize_url, extract_page_data
from urllib.parse import urljoin, urlsplit
from crawl import *

class AsyncCrawler:
    def __init__(self, base_url, max_concurrency, max_pages):
        self.base_url = base_url
        self.base_domain = urlsplit(base_url).netloc
        self.page_data = {}
        self.visited = set()
        self.lock = asyncio.Lock()
        self.max_concurrency: int = max_concurrency
        self.semaphore = asyncio.Semaphore(max_concurrency)
        self.session: aiohttp.ClientSession | None = None 
        self.max_pages: int = max_pages
        self.should_stop: bool = False
        self.all_tasks = set()

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session is not None:
            await self.session.close()

    async def add_page_visit(self, normalized_url: str):
        if self.should_stop is True:
            return False
        async with self.lock:
            if len(self.visited) >= self.max_pages:
                self.should_stop = True
                print("Reached maximum number of pages to crawl.")
                return False
            if normalized_url in self.visited:
                return False
            else:
                self.visited.add(normalized_url)
                return True

    async def get_html(self, url: str) -> str | None:
        try:
            if self.session is not None:
                async with self.session.get(url) as response:
                    if response.status > 399:
                        raise Exception(f"got HTTP error: {response.status} {response.reason}")
                    content_type = response.headers.get("content-type", "")
                    if "text/html" not in content_type:
                        raise Exception(f"got non-HTML response: {content_type}")
                    return await response.text()
        except Exception as e:
            raise Exception(f"network error while fetching {url}: {e}")


    async def crawl_page(self, current_url=None):
        task = asyncio.current_task()
        self.all_tasks.add(task)
        try:
            if self.should_stop is True:
                return
            if current_url == None:
                current_url = self.base_url
            if urlsplit(current_url).netloc != self.base_domain:
                return
            normalized_current_url = normalize_url(current_url)
            if await self.add_page_visit(normalized_current_url) is False:
                return
            async with self.semaphore:
                print(f"crawling {current_url}")
                current_html = await self.get_html(current_url)
            if current_html == None:
                return
            page_info = extract_page_data(current_html, current_url)
            async with self.lock:
                self.page_data[normalized_current_url] = page_info
            next_urls = page_info["outgoing_links"]
            tasks = []
            for next_url in next_urls:
                tasks.append(asyncio.create_task(self.crawl_page(next_url)))
            await asyncio.gather(*tasks)
        finally:
            self.all_tasks.discard(task)

    async def crawl(self):
        await self.crawl_page(self.base_url)
        return self.page_data

    