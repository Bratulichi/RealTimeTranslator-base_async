# from .tracing import TracingService
# from ..config import BrokerConfig
# from ..models.dumps import FormatDumps
# from ..models.events.base import BaseEvent, JsonEvent
import asyncio
import enum
import json
import typing as t
from contextlib import asynccontextmanager
from copy import deepcopy

import aio_pika
from aio_pika import Connection, Channel, Message, DeliveryMode
from aio_pika.abc import AbstractIncomingMessage

# from models.dumps import FormatDumps
from ..models.events.base import BaseEvent, JsonEvent
from ..сonfig import BrokerConfig

# from .tracing import TracingService
# from ..base_module import ClassesLoggerAdapter
from base_module import FastAPILoggerAdapter

_T_EVENT = t.TypeVar('_T_EVENT', bound=BaseEvent)



class _ReceiverSignature(t.Protocol):
    """Сигнатура получателя сообщений."""

    async def __call__(
            self,
            event: _T_EVENT,
            message: AbstractIncomingMessage,
            **kwargs
    ) -> t.Optional[bool]: ...


T_RECEIVER = t.TypeVar('T_RECEIVER', bound=_ReceiverSignature)


class Priorities(enum.IntEnum):
    """Приоритеты сообщений."""

    LEAST = 1
    LOW = 2
    MEDIUM = 3
    HIGH = 4
    HIGHEST = 5


class AsyncBrokerService:
    """Асинхронный сервис брокера сообщений на основе aio_pika."""

    @property
    def priorities(self) -> t.Type[Priorities]:
        return Priorities

    @property
    def config(self):
        return deepcopy(self._config)

    def __init__(self, config: BrokerConfig):
        """Инициализация сервиса."""
        self._config = config
        self._tracer = TracingService()
        self._consumer = _AsyncRabbitConsumer(config)
        self._logger = FastAPILoggerAdapter.create(self)
        self._connection: t.Optional[Connection] = None

    async def __aenter__(self):
        """Асинхронный контекстный менеджер - вход."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Асинхронный контекстный менеджер - выход."""
        await self.disconnect()

    async def connect(self):
        """Установка соединения с RabbitMQ."""
        if self._connection and not self._connection.is_closed:
            return

        connection_url = (
            f'amqp://{self._config.connection.user}:'
            f'{self._config.connection.password}@'
            f'{self._config.connection.host}:'
            f'{self._config.connection.port}/'
        )

        self._connection = await aio_pika.connect_robust(
            connection_url,
            heartbeat=60
        )

    async def disconnect(self):
        """Закрытие соединения."""
        if self._connection and not self._connection.is_closed:
            await self._connection.close()

    @asynccontextmanager
    async def _get_channel(self) -> t.AsyncContextManager[Channel]:
        """Получение канала для операций."""
        if not self._connection or self._connection.is_closed:
            await self.connect()

        async with self._connection.channel() as channel:
            yield channel

    async def declare_dlx(
            self,
            routing_key: str,
            dlx_queue_name: str,
            message_ttl_sec: int,
            dlx_exchange: str = '',
            max_priority: int = 5,
    ) -> tuple[str, str]:
        """Объявление очереди с dead letter exchange."""
        async with self._get_channel() as channel:
            dlx_name = dlx_queue_name + f'-{message_ttl_sec}s'
            await channel.declare_queue(
                name=dlx_name,
                durable=True,
                arguments={
                    'x-message-ttl': message_ttl_sec * 1000,
                    'x-dead-letter-exchange': dlx_exchange,
                    'x-dead-letter-routing-key': routing_key,
                    'x-max-priority': max_priority
                }
            )
            return dlx_exchange, dlx_name

    def _make_message_properties(
            self,
            priority: int = None,
            reply_to: str = None,
            **kwargs
    ) -> dict:
        """Создание свойств сообщения."""
        properties = {
            'delivery_mode': DeliveryMode.PERSISTENT, **kwargs
        }

        if reply_to or self._config.publisher.reply_to:
            properties['reply_to'] = reply_to \
                                     or self._config.publisher.reply_to

        if priority is not None:
            properties['priority'] = priority

        return properties

    async def publish(
            self,
            message: BaseEvent,
            routing_key: str = None,
            exchange: str = None,
            priority: int = None,
            **message_kwargs
    ) -> bool:
        """Публикация одного сообщения."""
        routing_key = routing_key or self._config.publisher.routing_key
        exchange_name = exchange or self._config.publisher.exchange

        try:
            async with self._get_channel() as channel:
                # Устанавливаем trace_id
                message.trace_id = self._tracer.trace_id

                # Получаем exchange
                exchange_obj = await channel.get_exchange(
                    exchange_name) if exchange_name else channel.default_exchange

                # Подготавливаем сообщение
                message_body = json.dumps(message, cls=FormatDumps).encode()
                properties = self._make_message_properties(
                    priority=priority, **message_kwargs
                )

                aio_message = Message(
                    body=message_body,
                    **properties
                )

                # Публикуем сообщение
                await exchange_obj.publish(
                    message=aio_message,
                    routing_key=routing_key
                )

                self._logger.info(
                    'Отправлено сообщение',
                    extra={'routing_key': routing_key,
                           'exchange': exchange_name}
                )
                return True

        except Exception as e:
            self._logger.error(
                'Ошибка отправки сообщения',
                extra={
                    'message': message,
                    'exchange': exchange_name,
                    'routing_key': routing_key,
                    'priority': priority,
                    'e': e
                },
                exc_info=True
            )
            return False

    async def publish_many(
            self,
            messages: t.List[BaseEvent],
            routing_key: str = None,
            exchange: str = None,
            priority: int = None,
            **message_kwargs
    ) -> bool:
        """Публикация множества сообщений."""
        routing_key = routing_key or self._config.publisher.routing_key
        exchange_name = exchange or self._config.publisher.exchange

        try:
            async with self._get_channel() as channel:
                # Получаем exchange
                exchange_obj = await channel.get_exchange(
                    exchange_name) if exchange_name else channel.default_exchange

                # Подготавливаем свойства сообщений
                properties = self._make_message_properties(
                    priority=priority, **message_kwargs
                )

                # Публикуем все сообщения
                for message in messages:
                    message.trace_id = self._tracer.trace_id
                    message_body = json.dumps(
                        message, cls=FormatDumps
                    ).encode()

                    aio_message = Message(body=message_body, **properties)

                    await exchange_obj.publish(
                        message=aio_message,
                        routing_key=routing_key
                    )

                    self._logger.info(
                        'Отправлено сообщение',
                        extra={
                            'routing_key': routing_key,
                            'exchange': exchange_name
                        }
                    )

                return True

        except Exception as e:
            self._logger.error(
                'Ошибка отправки сообщений',
                extra={
                    'messages': messages,
                    'exchange': exchange_name,
                    'routing_key': routing_key,
                    'priority': priority,
                    'e': e
                },
                exc_info=True
            )
            return False

    async def start_consuming(
            self,
            receiver: _ReceiverSignature,
            event_type: t.Type[BaseEvent] = JsonEvent
    ):
        """Запуск потребления сообщений."""
        while True:
            try:
                self._logger.info(
                    'Запуск прослушивания очереди',
                    extra={'queue': self._config.consumer.queue_name}
                )
                await self._consumer.consume(receiver, event_type)
            except Exception as e:
                self._logger.error(
                    'Ошибка подключения или прослушивания очереди',
                    extra={'queue': self._config.consumer.queue_name, 'e': e},
                    exc_info=True
                )

            await asyncio.sleep(self._config.consumer.error_timeout)

    async def consume_once(
            self,
            receiver: _ReceiverSignature,
            event_type: t.Type[BaseEvent] = JsonEvent
    ):
        """Одноразовое потребление сообщений (без вечного цикла)."""
        await self._consumer.consume(receiver, event_type)


class _AsyncRabbitConsumer:
    """Асинхронный потребитель RabbitMQ."""

    def __init__(self, config: BrokerConfig):
        """Инициализация потребителя."""
        self._config = config
        self._tracer = TracingService()
        self._logger = FastAPILoggerAdapter.create(self)
        self._connection: t.Optional[Connection] = None
        self._should_stop = False

    async def _connect(self) -> Connection:
        """Подключение к RabbitMQ."""
        if self._connection and not self._connection.is_closed:
            return self._connection

        connection_url = (
            f'amqp://{self._config.connection.user}:'
            f'{self._config.connection.password}@'
            f'{self._config.connection.host}:'
            f'{self._config.connection.port}/'
        )

        self._connection = await aio_pika.connect_robust(
            connection_url,
            heartbeat=60
        )
        return self._connection

    async def _setup_queue(self, channel: Channel):
        """Настройка очереди."""
        priority = self._config.consumer.max_priority
        queue = await channel.declare_queue(
            name=self._config.consumer.queue_name,
            durable=True,
            arguments={'x-max-priority': priority}
        )
        await channel.set_qos(prefetch_count=1)
        return queue

    async def _process_message(
            self,
            receiver: T_RECEIVER,
            event_type: t.Type[BaseEvent],
            message: AbstractIncomingMessage
    ):
        """Обработка одного сообщения."""
        try:
            # Парсим сообщение
            try:
                body = message.body.decode()
                event = event_type.load(json.loads(body))
            except Exception as e:
                self._logger.warning(
                    'Сообщение не подходит под формат',
                    extra={'body': message.body, 'e': e}
                )
                await message.nack(requeue=False)
                return

            # Обрабатываем с трассировкой
            try:
                async with self._tracer.trace(event.trace_id):
                    handling_error = await receiver(
                        event=event,
                        message=message
                    )

                    if handling_error:
                        await message.nack(requeue=True)
                    else:
                        await message.ack()

            except Exception as e:
                self._logger.error(
                    'Ошибка обработки сообщения',
                    exc_info=True,
                    extra={'body': message.body, 'e': e}
                )
                await message.nack(requeue=False)

        except Exception as e:
            self._logger.error(
                'Критическая ошибка при обработке сообщения',
                exc_info=True,
                extra={'e': e}
            )

    async def consume(
            self,
            receiver: T_RECEIVER,
            event_type: t.Type[BaseEvent]
    ):
        """Запуск потребления сообщений."""
        connection = await self._connect()

        try:
            async with connection.channel() as channel:
                queue = await self._setup_queue(channel)

                # Настройка обработчика сообщений
                async def message_handler(message: AbstractIncomingMessage):
                    await self._process_message(receiver, event_type, message)

                # Запуск потребления
                await queue.consume(message_handler, no_ack=False)

                self._logger.debug(
                    'Потребитель запущен, ожидание сообщений...')

                # Ожидание завершения
                try:
                    await asyncio.Future()  # Будет работать до отмены
                except asyncio.CancelledError:
                    self._logger.info('Потребление остановлено')
                    raise

        except Exception as e:
            self._logger.error(
                'Ошибка в процессе потребления',
                exc_info=True,
                extra={'e': e}
            )
            raise
        finally:
            if connection and not connection.is_closed:
                await connection.close()

    def stop(self):
        """Остановка потребления."""
        self._should_stop = True
