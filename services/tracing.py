import typing as t
from uuid import uuid4
from contextlib import contextmanager

from fastapi import FastAPI, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from base_module import FastAPILoggerAdapter


class TracingService:
    TRACE_HEADER = "X-Trace-Id"

    @property
    def trace_id(self) -> str:
        return FastAPILoggerAdapter.TRACE_ID.get()

    @classmethod
    @contextmanager
    def trace(cls, trace_id: str):
        cls.receive({cls.TRACE_HEADER: trace_id})
        try:
            yield
        finally:
            cls.reset()

    @classmethod
    def reset(cls):
        FastAPILoggerAdapter.TRACE_ID.set(
            FastAPILoggerAdapter.DEFAULT_TRACE_ID
        )

    @classmethod
    def receive(cls, gettable: t.Mapping):
        trace_id = gettable.get(cls.TRACE_HEADER, uuid4().hex)
        FastAPILoggerAdapter.TRACE_ID.set(trace_id)

    @classmethod
    def emit(cls, settable: t.MutableMapping):
        trace_id = FastAPILoggerAdapter.TRACE_ID.get()
        settable[cls.TRACE_HEADER] = trace_id

    # --- FastAPI integration ---
    @classmethod
    async def tracing_middleware(cls, request: Request, call_next):
        # Получаем trace_id из заголовков
        cls.receive(request.headers)

        # Выполняем обработку запроса
        response: Response = await call_next(request)

        # Добавляем trace_id в заголовки ответа
        cls.emit(response.headers)
        return response

    @classmethod
    def setup_fastapi_tracing(cls, app: FastAPI):
        app.add_middleware(BaseHTTPMiddleware, dispatch=cls.tracing_middleware)
