import ijson
import ujson
import asyncio
import aiofiles
from typing import Optional
import argparse
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import logging
from copy import deepcopy
import sys
import os


class Loader:
    def __init__(
        self, 
        elastic_address: str, 
        index: str, 
        input_dir: str, 
        chunk_size: int = 500,
        limit: Optional[int] = None
    ):
        self.ijson_backend = ijson.get_backend("yajl2_c")
        self.elastic_address = elastic_address
        self.index = index
        self.input_dir = input_dir
        self.chunk_size = chunk_size
        self.limit = limit

        self.logger = logging.getLogger('Loader')
        self.logger.setLevel(logging.INFO)

    async def __aenter__(self):
        self.es = AsyncElasticsearch(hosts=self.elastic_address)
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        return self

    async def __aexit__(self, *exc_info):
        await self.es.close()
        [h.close() for h in self.logger.handlers]

    async def start(self):
        await self.create_index_with_meta()
        await self.upload_data()

    async def create_index_with_meta(self):
        async with aiofiles.open(os.path.join(self.input_dir, 'settings.json'), 'r') as sf,\
                aiofiles.open(os.path.join(self.input_dir, 'mappings.json'), 'r') as mf:
            settings, mappings = await asyncio.gather(sf.readline(), mf.readline())
        
        settings = ujson.loads(settings)['index']
        settings.pop('routing')
        settings.pop('provided_name')
        settings.pop('creation_date')
        settings.pop('uuid')
        settings.pop('version')

        mappings = ujson.loads(mappings)
        res = await self.es.indices.create(self.index, body={"settings": settings, "mappings": mappings})
        self.logger.info('Index created sucessfully' if res.get('acknowledged') else 'Index creation failed')

    async def upload_data(self):
        inserts = []
        async with aiofiles.open(os.path.join(self.input_dir, 'data.json'), 'r') as f:
            actions, i = [], 0
            async for obj in self.ijson_backend.items_async(f, 'item'):
                i += 1
                action = {"_index": self.index, "_id": str(obj['_id']), "_source": obj['_source']}
                actions.append(action)
                if i%500 == 0:
                    inserts.append(asyncio.create_task(async_bulk(self.es, deepcopy(actions))))
                    actions.clear()
        if actions:
            inserts.append(asyncio.create_task(async_bulk(self.es, deepcopy(actions))))
        await asyncio.wait(inserts)
        self.logger.info('Data upload finished')


async def main():
    async with Loader(
            elastic_address=args.elastic_address,
            index=args.index,
            input_dir=args.input_dir,
            limit=args.limit) as loader:
        await loader.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--elastic_address', type=str, required=True)
    parser.add_argument('--index', type=str, required=True)
    parser.add_argument('--input_dir', type=str, required=True)
    parser.add_argument('--limit', type=int, required=False, default=None)
    args = parser.parse_args()

    asyncio.run(main())