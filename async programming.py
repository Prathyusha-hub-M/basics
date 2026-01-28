import asyncio
import aiohttp

async def fetch_data(session, url):
    async with session.get(url) as response:
        return await response.text()

async def main():
    urls =[
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3"
    ]

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_data(session, url) for url in urls]

        result = await asyncio.gather(*tasks)
        print(len(result))

    asyncio.run(main())