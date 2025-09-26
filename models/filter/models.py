import typing as t
from enum import Enum

from pydantic import Field, field_validator, model_validator
from model import Model

class Op(Enum):
    """Операторы сравнения с поддержкой строкового парсинга"""
    EQ = ('=', lambda c, v: c == v, ['=', 'eq'])
    NE = ('!=', lambda c, v: c != v, ['!=', 'ne', 'not_eq'])
    GT = ('>', lambda c, v: c > v, ['>', 'gt'])
    GTE = ('>=', lambda c, v: c >= v, ['>=', 'gte', 'ge'])
    LT = ('<', lambda c, v: c < v, ['<', 'lt'])
    LTE = ('<=', lambda c, v: c <= v, ['<=', 'lte', 'le'])
    IN = ('in', lambda c, v: c.in_(v if isinstance(v, list) else [v]), ['in', 'IN'])
    NOT_IN = ('not_in', lambda c, v: ~c.in_(v if isinstance(v, list) else [v]), ['not_in', 'NOT_IN'])
    LIKE = ('like', lambda c, v: c.like(f'%{v}%'), ['like', 'LIKE'])
    ILIKE = ('ilike', lambda c, v: c.ilike(f'%{v}%'), ['ilike', 'ILIKE'])

    def __init__(
            self, symbol: str, operation: t.Callable, aliases: list[str]
    ):
        self.symbol = symbol
        self.operation = operation
        self.aliases = aliases

    def apply(self, column, value):
        """Применяет операцию к колонке и значению"""
        return self.operation(column, value)

    def __str__(self):
        return self.symbol

    @classmethod
    def from_string(cls, op_str: str) -> 'Op':
        """Преобразует строку в оператор"""
        for op in cls:
            if op_str in op.aliases:
                return op
        raise ValueError(
            f"Неподдерживаемый оператор: '{op_str}'. Доступные: {cls.get_all_aliases()}")

    @classmethod
    def get_all_aliases(cls) -> list[str]:
        """Возвращает все доступные алиасы операторов"""
        aliases = []
        for op in cls:
            aliases.extend(op.aliases)
        return aliases


class Filter(Model):
    """Фильтр поля"""
    name: str = Field(..., min_length=1)
    op: Op | str = Field(default=Op.EQ)
    value: t.Any = Field(...)
    type: t.Literal['str', 'int', 'float', 'bool', 'date'] | None = None

    @field_validator('name')
    @classmethod
    def validate_name(cls, v):
        if not v or not v.strip():
            raise ValueError('Название поля не может быть пустым')
        if not v.replace('_', '').replace('-', '').isalnum():
            raise ValueError(
                f"Недопустимые символы в названии поля: '{v}'")
        return v.strip()

    @field_validator('op')
    @classmethod
    def validate_op(cls, v):
        if isinstance(v, str):
            return Op.from_string(v)
        return v

    @field_validator('value')
    @classmethod
    def validate_value(cls, v):
        if v is None:
            raise ValueError('Значение фильтра не может быть None')
        return v


class FilterGroup(Model):
    """Группа фильтров"""
    op: t.Literal['and', 'or'] = 'and'
    filters: list[Filter] = Field(..., min_length=1)

    @field_validator('op')
    @classmethod
    def validate_group_op(cls, v):
        op_mapping = {
            'and': 'and', 'AND': 'and', 'all': 'and',
            'or': 'or', 'OR': 'or', 'any': 'or'
        }
        if v not in op_mapping:
            raise ValueError(f"Неподдерживаемый оператор группы: '{v}'")
        return op_mapping[v]


class FilterQuery(Model):
    """Запрос с фильтрацией через FilterQuery объекты"""
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=1000)
    order_by: str | None = None
    desc: bool = False
    groups: list[FilterGroup] = Field(default_factory=list)

    @field_validator('order_by')
    @classmethod
    def validate_order_by(cls, v):
        if v is not None:
            v = v.strip()
            if v and not v.replace('_', '').replace('-', '').isalnum():
                raise ValueError(
                    f"Недопустимые символы в поле сортировки: '{v}'")
        return v


class FilterParams(Model):
    """Параметры фильтрации из JSON"""
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=1000)
    order_by: str | None = None
    asc: int = Field(default=1)
    filters: dict[str, t.Any] | list[dict[str, t.Any]] | None = Field(
        default_factory=dict
    )
    advanced_filters: list[dict[str, t.Any]] | None = Field(
        default_factory=list
    )

    @field_validator('asc')
    @classmethod
    def validate_asc(cls, v):
        if v not in [0, 1]:
            raise ValueError("Параметр 'asc' должен быть 0 или 1")
        return v

    @field_validator('order_by')
    @classmethod
    def validate_order_by(cls, v):
        if v is not None:
            v = v.strip()
            if v and not v.replace('_', '').replace('-', '').isalnum():
                raise ValueError(
                    f"Недопустимые символы в поле сортировки: '{v}'")
        return v

    @model_validator(mode='after')
    def normalize_and_validate_filters(self):
        """Нормализует и валидирует фильтры"""
        if isinstance(self.filters, list):
            if not self.advanced_filters:
                self.advanced_filters = []

            # Валидируем структуру
            for i, filter_group in enumerate(self.filters):
                if not isinstance(filter_group, dict):
                    raise ValueError(
                        f'Элемент filters[{i}] должен быть объектом')
                if 'operator' not in filter_group:
                    raise ValueError(
                        f"В filters[{i}] отсутствует поле 'operator'")
                if 'items' not in filter_group:
                    raise ValueError(
                        f"В filters[{i}] отсутствует поле 'items'")
                if not isinstance(filter_group['items'], list) or not \
                        filter_group['items']:
                    raise ValueError(
                        f"Поле 'items' в filters[{i}] должно быть непустым списком")

                for j, item in enumerate(filter_group['items']):
                    if not isinstance(item, dict):
                        raise ValueError(
                            f'Элемент filters[{i}].items[{j}] должен быть объектом')
                    if 'name' not in item:
                        raise ValueError(
                            f"В filters[{i}].items[{j}] отсутствует поле 'name'")
                    if 'value' not in item:
                        raise ValueError(
                            f"В filters[{i}].items[{j}] отсутствует поле 'value'")

            self.advanced_filters.extend(self.filters)
            self.filters = {}

        elif isinstance(self.filters, dict):
            for key, value in self.filters.items():
                if not key or not key.strip():
                    raise ValueError('Ключ фильтра не может быть пустым')
                if value is None:
                    raise ValueError(
                        f"Значение фильтра для поля '{key}' не может быть None")

        return self

    @property
    def is_desc(self) -> bool:
        return self.asc == 0

    def has_only_pagination_and_sorting(self) -> bool:
        """Проверяет, есть ли только параметры пагинации и сортировки без фильтров"""
        has_simple = isinstance(self.filters, dict) and bool(self.filters)
        has_advanced = bool(self.advanced_filters)
        return not (has_simple or has_advanced)

    def has_any_filters(self) -> bool:
        """Проверяет, есть ли хоть какие-то фильтры"""
        has_simple = isinstance(self.filters, dict) and bool(self.filters)
        has_advanced = bool(self.advanced_filters)
        return has_simple or has_advanced


class FilterResult(Model):
    """Результат фильтрации"""
    items: list[t.Any]
    total: int = Field(ge=0)


class FilterConfig:
    """Конфигурация фильтрации"""

    def __init__(
            self,
            allowed_fields: list[str] | None = None,
            default_limit: int = 50,
            max_limit: int = 1000,
            default_order_by: str | None = None
    ):
        self.allowed_fields = set(
            allowed_fields) if allowed_fields else None
        self.default_limit = default_limit
        self.max_limit = max_limit
        self.default_order_by = default_order_by