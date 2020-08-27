import asyncio

from base_async_pipeline import BaseAsyncPipeline
from super_mario import process_pipe


def test_different_async_calls():
    class SimplePipeline(BaseAsyncPipeline):
        pipeline = [
            'sum_numbers',
            'sum_numbers_again',
            'multiply_numbers',
        ]

        @staticmethod
        @process_pipe
        async def sum_numbers(a, b):
            return {'d': a + b}

        @process_pipe
        @staticmethod
        async def sum_numbers_again(d):
            return {'e': d + 2}

        @process_pipe
        async def multiply_numbers(e, d):
            return {'f': e * d}

    result = asyncio.get_event_loop().run_until_complete(SimplePipeline().run(a=2, b=3))

    assert result == 35
