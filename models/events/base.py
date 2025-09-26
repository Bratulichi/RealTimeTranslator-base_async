# import dataclasses as dc
# import typing
import typing as t

from sqlmodel import Field

from ...base_module import Model, FastAPILoggerAdapter

TV_PAYLOAD = t.TypeVar('TV_PAYLOAD')


class BaseEvent(Model, t.Generic[TV_PAYLOAD]):
    """."""

    T: t.ClassVar

    payload: TV_PAYLOAD = Field
    trace_id: str = Field(default=FastAPILoggerAdapter.DEFAULT_TRACE_ID)
    ttl: t.Optional[int] = Field(default=0)

    @classmethod
    def lazy_load(cls, payload: TV_PAYLOAD, **kwargs):
        return cls(
            payload=payload,
            trace_id=FastAPILoggerAdapter.TRACE_ID.get(),
            **kwargs
        )


class _TaskIdentProto(t.Protocol):
    """."""

    task_id: int


class _TaskIdentModel(Model):
    """."""

    task_id: int = Field()


class TaskIdentEvent(BaseEvent[_TaskIdentModel]):
    """."""

    T = _TaskIdentModel

    @classmethod
    def lazy_load(cls, payload: TV_PAYLOAD | int | _TaskIdentProto, **kwargs):
        if isinstance(payload, int):
            payload = _TaskIdentModel(task_id=payload)
        elif hasattr(payload, 'task_id'):
            payload = _TaskIdentModel(task_id=getattr(payload, 'task_id'))

        return cls(
            payload=payload,
            trace_id=FastAPILoggerAdapter.TRACE_ID.get(),
            **kwargs
        )


class JsonEvent(BaseEvent[dict]):
    """."""

    T = dict


class ModelEvent(BaseEvent[Model]):
    """."""

    T = Model

    @classmethod
    def lazy_load(cls, payload: TV_PAYLOAD, **kwargs):
        return cls(
            payload=cls.T.load(payload),
            trace_id=FastAPILoggerAdapter.TRACE_ID.get(),
            **kwargs
        )
