import typing as t
from contextlib import asynccontextmanager

from pydantic import ValidationError
from sqlmodel.ext.asyncio.session import AsyncSession

from base_module import EXC, ErrorCode
from base_module import FastAPILoggerAdapter
from injectors import AsyncPgConnectionInj
from models import (
    FilterConfig,
    AsyncQueryFilterEngine,
    FilterQuery,
    FilterParams,
    FilterResult,
)
from base_module import Model


class FilterService:
    """Async сервис для работы с фильтрацией"""

    def __init__(self, pg: AsyncPgConnectionInj) -> None:
        self._pg = pg
        self._logger = FastAPILoggerAdapter.create(self)

    @asynccontextmanager
    async def _get_session(
            self, session: AsyncSession | None = None
    ) -> t.AsyncIterator[AsyncSession]:
        if session is not None:
            yield session
        else:
            if not self._pg:
                raise EXC(ErrorCode.DbError)

            async with self._pg.acquire_session() as new_session:
                yield new_session

    @classmethod
    def _get_engine(
            cls,
            model: type[Model],
            engine: AsyncQueryFilterEngine | None,
            config: FilterConfig | None = None,
    ) -> AsyncQueryFilterEngine:
        if not engine:
            return AsyncQueryFilterEngine(model, config)
        else:
            return engine

    async def filter_with_objects(
            self,
            model: type[Model],
            filter_query: FilterQuery,
            engine: AsyncQueryFilterEngine | None = None,
            session: AsyncSession | None = None,
    ) -> FilterResult:
        """Фильтрация через объекты классов"""
        engine = self._get_engine(model, engine)
        async with self._get_session(session) as session:
            return await engine.filter_by_query(filter_query, session)

    async def filter_with_json(
            self,
            model: type[Model],
            json_data: dict,
            engine: AsyncQueryFilterEngine | None = None,
            session: AsyncSession | None = None,
    ) -> FilterResult:
        """Фильтрация через JSON данные"""
        try:
            # Пустой JSON - возвращаем с базовой пагинацией
            if not json_data:
                engine = self._get_engine(model, engine)
                async with self._get_session(session) as session:
                    return await engine.paginate(session, page=1, size=50)

            engine = self._get_engine(model, engine)
            async with self._get_session(session) as session:
                if 'groups' in json_data:
                    # Формат FilterQuery
                    query = FilterQuery(**json_data)
                    return await engine.filter_by_query(query, session)
                else:
                    # Формат FilterParams
                    params = FilterParams(**json_data)

                    # Если есть только пагинация и сортировка без фильтров - это валидно
                    if params.has_only_pagination_and_sorting():
                        filter_query = FilterQuery(
                            offset=params.offset,
                            limit=params.limit,
                            order_by=params.order_by,
                            desc=params.is_desc,
                            groups=[]
                        )
                        return await engine.filter_by_query(
                            filter_query, session
                        )

                    return await engine.filter_by_params(params, session)

        except ValidationError as e:
            # Простая обработка ошибок Pydantic
            error_messages = [error['msg'] for error in e.errors()]
            raise EXC(
                ErrorCode.ValidationError,
                details={
                    'errors': error_messages,
                    'details': e.errors()
                }
            )
        except ValueError as e:
            # Обработка ошибок валидации полей из Engine
            raise EXC(
                ErrorCode.ValidationError,
                details={'reason': str(e)}
            )

    async def quick_filter(
            self,
            model: type[Model],
            session: AsyncSession | None = None,
            engine: AsyncQueryFilterEngine | None = None,
            **filters
    ) -> FilterResult:
        """Быстрая фильтрация по ключ=значение"""
        engine = self._get_engine(model, engine)
        async with self._get_session(session) as session:
            return await engine.quick_filter(session, **filters)

    async def paginate(
            self,
            model: type[Model],
            session: AsyncSession | None = None,
            page: int = 1,
            size: int = 20,
            order_by: str | None = None,
            engine: AsyncQueryFilterEngine | None = None,
            desc: bool = False
    ) -> FilterResult:
        """Простая пагинация"""
        engine = self._get_engine(model, engine)
        async with self._get_session(session) as session:
            return await engine.paginate(session, page, size, order_by,
                                         desc)