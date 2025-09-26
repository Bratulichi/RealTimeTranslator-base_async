import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.engine.url import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine, async_scoped_session,
    async_sessionmaker, create_async_engine
)
from sqlalchemy_utils import create_database, database_exists
from sqlmodel import text
from sqlmodel.ext.asyncio.session import AsyncSession

from base_module import (
    FastAPILoggerAdapter,
    PgConfig,
)
from models import BaseOrmModel


class AsyncPgConnectionInj:
    def __init__(self, conf: PgConfig, **pool_kwargs) -> None:
        self._conf = conf
        self._pool_config = {
            'pool_size': 20,
            'max_overflow': 30,
            'pool_timeout': 60,
            'pool_recycle': 3600,
            'pool_pre_ping': True,
            **pool_kwargs
        }
        self._session_factory = None
        self._engine: AsyncEngine | None = None
        self._logger = FastAPILoggerAdapter.create(self)
        self._is_setup = False

    def _build_url(self, async_driver=True) -> URL:
        driver = 'postgresql+asyncpg' if async_driver else 'postgresql'
        return URL.create(
            driver,
            username=self._conf.user,
            password=self._conf.password,
            host=self._conf.host,
            port=self._conf.port,
            database=self._conf.database,
        )

    async def _ensure_db_exists(self) -> None:
        """Проверка и создание базы данных если не существует."""
        try:
            if not database_exists(self._build_url(False)):
                create_database(self._build_url(False))
        except Exception as e:
            self._logger.warn(
                f'Не удаётся найти/создать базу данных.',
                extra={'e': e}
            )

    async def _create_engine_and_schema(self) -> AsyncEngine:
        """Создание движка и схемы БД."""
        engine = create_async_engine(
            self._build_url(),
            echo=self._conf.debug,
            **self._pool_config
        )

        try:
            async with (engine.begin() as conn):
                if hasattr(
                        self._conf, 'db_schema'
                ) and self._conf.db_schema:
                    await conn.execute(text(
                        f'CREATE SCHEMA IF NOT EXISTS {self._conf.db_schema}'
                    ))

                # Создаем таблицы
                await conn.run_sync(BaseOrmModel.metadata.create_all)
        except Exception as e:
            self._logger.warn(
                f'Ошибка создания схемы/таблицы.',
                extra={'e': e}
            )

        return engine

    async def setup(self) -> None:
        """Инициализация подключения к БД."""
        if self._is_setup:
            return

        try:
            await self._ensure_db_exists()
            self._engine = await self._create_engine_and_schema()

            session_maker = async_sessionmaker(
                self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
            )

            self._session_factory = async_scoped_session(
                session_maker,
                scopefunc=asyncio.current_task
            )

            self._is_setup = True
            self._logger.info(
                f'PostgreSQL подключение создано.',
                extra={
                    'host': self._conf.host,
                    'port': self._conf.port,
                    'database': self._conf.database
                }
            )

        except Exception as e:
            self._logger.error(
                f'Ошибка при создании PostgreSQL подключения.',
                extra={'e': e}
            )
            raise

    @asynccontextmanager
    async def acquire_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Получение сессии БД с автоматическим управлением."""
        if not self._is_setup:
            await self.setup()

        if not self._session_factory:
            raise RuntimeError('Не удалось создать фабрику сессий')

        try:
            async with self._session_factory() as session:
                # Устанавливаем роль если указана в конфиге
                if hasattr(self._conf, 'user') and self._conf.user:
                    try:
                        await session.execute(
                            text(f'SET ROLE {self._conf.user}'))
                    except Exception:
                        # Игнорируем ошибки установки роли - не критично
                        pass

                yield session

        except Exception as e:
            self._logger.warn(
                f'Ошибка сессии базы данных.', extra={'e': e}
            )
            raise

    async def disconnect(self) -> None:
        """Закрытие всех подключений."""
        try:
            if self._session_factory:
                await self._session_factory.remove()
                self._session_factory = None

            if self._engine:
                await self._engine.dispose()
                self._engine = None

            self._is_setup = False
            self._logger.info('PostgreSQL подключение успешно закрыто')

        except Exception as e:
            self._logger.warn(
                f'Внимание: Ошибка при отключении соединения PostgreSQL',
                extra={'e': e},
            )

        return self._is_setup and self._engine is not None
