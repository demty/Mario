import asyncio
import logging
from copy import deepcopy
from typing import Any

from super_mario.exceptions import ProgrammingException
from super_mario import BasePipeline
from super_mario.base_pipeline import ContextType
from super_mario.utils.formatters import format_object_for_logging

logger = logging.getLogger(__name__)


class BaseAsyncPipeline(BasePipeline):
    async def _handle_pipeline(self) -> ContextType:
        result = None
        for pipe_name in self.pipeline:
            pipe = getattr(self, pipe_name)
            pipe_args = self._get_pipe_args(pipe)

            logger.debug(
                f'Executing {pipe_name} with {format_object_for_logging(pipe_args)}...',
            )
            result = await pipe(**pipe_args)
            logger.debug(f'\t{pipe_name} finished')

            if result:
                self._validate_pipe(result, pipe_name)
                self.__context__.update(result)
        return result

    def _validate_whole_pipeline_is_async_raise_on_error(self) -> None:
        for pipe_name in self.pipeline:
            pipe = getattr(self, pipe_name)
            if not asyncio.iscoroutinefunction(pipe):
                raise ProgrammingException(
                    f'Pipe {pipe_name} is not asynchronous',
                )

    async def run(self, **kwargs: Any) -> ContextType:
        self._validate_run_arguments_raise_on_error(kwargs)
        self._validate_whole_pipeline_is_async_raise_on_error()
        self.__context__ = deepcopy(kwargs)
        result = await self._handle_pipeline()
        return list(result.values())[0] if result else None
