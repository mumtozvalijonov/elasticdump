import asyncio
from elasticsearch import AsyncElasticsearch
import aiofiles
from typing import Optional
import ujson
import os
import argparse


class Dumper:

    def __init__(
        self, 
        elastic_address: str, 
        index: str, 
        output_dir: str, 
        chunk_size: int = 500,
        limit: Optional[int] = None
    ):
        self.elastic_address = elastic_address
        self.index = index
        self.output_dir = output_dir
        self.chunk_size = chunk_size
        self.limit = limit

    async def __aenter__(self):
        self.es = AsyncElasticsearch(hosts=self.elastic_address)
        return self

    async def __aexit__(self, *exc_info):
        await self.es.close()

    async def dump_meta(self):
        meta = (await self.es.indices.get(self.index))[self.index]
        async with aiofiles.open(os.path.join(self.output_dir, 'mappings.json'), 'w') as mf, \
                aiofiles.open(os.path.join(self.output_dir, 'settings.json'), 'w') as sf:
            await asyncio.gather(
                mf.write(ujson.dumps(meta['mappings'])),
                sf.write(ujson.dumps(meta['settings']))
            )

    async def dump_data(self):
        count = self.limit or int((await self.es.cat.count(index=self.index)).strip().split()[-1])
        start = 0
        async with aiofiles.open(os.path.join(self.output_dir, 'data.json'), 'w') as f:
            await f.write('[')
            while start < count:
                documents = []
                tasks = []
                for _ in range(10):
                    new_start = min(start + self.chunk_size, count)
                    size = new_start - start
                    tasks.append(self.es.search(index=self.index, body={'size': size, 'from': start}))
                    start = new_start
                    if start >= count:
                        break
                results = await asyncio.gather(*tasks)
                for data in results:
                    documents.extend(data['hits']['hits'])
                    
                await f.write(ujson.dumps(documents)[1:-2])
                if start < count:
                    await f.write(',')
                else:
                    await f.write('}]')
            
    async def start(self):
        meta_task = asyncio.create_task(self.dump_meta())
        data_task = asyncio.create_task(self.dump_data())
        await asyncio.wait([meta_task, data_task])


async def main():
    async with Dumper(
            elastic_address=args.elastic_address, 
            index=args.index,
            output_dir=args.output_dir,
            limit=args.limit) as dumper:
        await dumper.start()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--elastic_address', type=str, required=True)
    parser.add_argument('--index', type=str, required=True)
    parser.add_argument('--output_dir', type=str, required=True)
    parser.add_argument('--limit', type=int, required=False, default=None)
    args = parser.parse_args()

    asyncio.run(main())