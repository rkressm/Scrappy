from async_crawler import AsyncCrawler

async def crawl_site_async(base_url, max_concurrency):
    async with AsyncCrawler(base_url, max_concurrency) as crawler:
        crawled_data = await crawler.crawl()
    return crawled_data