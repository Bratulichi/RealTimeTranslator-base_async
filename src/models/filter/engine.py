"""
Исправленный async класс-ориентированный движок фильтрации
"""
import typing as t
from datetime import datetime

from sqlalchemy import Select, func, and_, or_
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
from model import Model
from .models import (
    FilterQuery,
    FilterParams,
    FilterResult,
    Filter,
    FilterGroup,
    Op,
    FilterConfig
)


class AsyncQueryFilterEngine:
    """Основной класс для работы с async фильтрацией"""

    def __init__(
            self,
            model: type[Model],
            config: FilterConfig | None = None,
    ) -> None:
        self._model = model
        self._config = config or FilterConfig()
        self._model_fields = self._get_model_fields()

    def _get_model_fields(self) -> set:
        """Получает доступные поля модели"""
        fields = set()

        # Поля из SQLModel
        if hasattr(self._model, 'model_fields'):
            fields.update(self._model.model_fields.keys())

        # Поля из SQLAlchemy
        if hasattr(self._model, '__table__'):
            fields.update(self._model.__table__.columns.keys())

        # Дополнительно проверяем атрибуты
        for attr_name in dir(self._model):
            if hasattr(self._model,
                       attr_name) and not attr_name.startswith('_'):
                try:
                    attr = getattr(self._model, attr_name)
                    if hasattr(attr, 'type'):  # SQLAlchemy column
                        fields.add(attr_name)
                except:
                    pass

        return fields

    def _validate_field(self, field_name: str) -> bool:
        """Валидирует поле"""
        if self._config.allowed_fields:
            return field_name in self._config.allowed_fields
        return field_name in self._model_fields

    @classmethod
    def _cast_value(cls, value: t.Any, type_hint: str | None) -> t.Any:
        """Приведение типов"""
        if not type_hint or value is None:
            return value

        converters = {
            'str': str,
            'int': int,
            'float': float,
            'bool': lambda x: str(x).lower() in ('true', '1', 'yes', 'on'),
            'date': lambda x: datetime.fromisoformat(
                str(x).replace('Z', '+00:00')).date()
        }

        try:
            return converters[type_hint](value)
        except (KeyError, ValueError, TypeError):
            return value

    def _build_condition(self, filter_item: Filter):
        """Построение условия для фильтра"""
        if not self._validate_field(filter_item.name):
            return None

        try:
            column = getattr(self._model, filter_item.name)
            value = self._cast_value(filter_item.value, filter_item.type)
            return filter_item.op.apply(column, value)
        except AttributeError:
            return None

    def _apply_filter_groups(self, query: Select,
                             groups: list[FilterGroup]) -> Select:
        """Применяет группы фильтров"""
        for group in groups:
            conditions = []

            for filter_item in group.filters:
                condition = self._build_condition(filter_item)
                if condition is not None:
                    conditions.append(condition)

            if conditions:
                if group.op == 'and':
                    group_condition = and_(*conditions)
                else:  # or
                    group_condition = or_(*conditions)

                query = query.where(group_condition)

        return query

    def _apply_sorting(
            self, query: Select, order_by: str | None, desc: bool
    ) -> Select:
        """Применяет сортировку"""
        if order_by and self._validate_field(order_by):
            column = getattr(self._model, order_by)
            if desc:
                query = query.order_by(column.desc())
            else:
                query = query.order_by(column.asc())
        elif self._config.default_order_by and self._validate_field(
                self._config.default_order_by):
            column = getattr(self._model, self._config.default_order_by)
            query = query.order_by(column.asc())

        return query

    def _apply_pagination(
            self, query: Select, offset: int, limit: int
    ) -> Select:
        """Применяет пагинацию"""
        # Ограничиваем лимит
        limit = min(limit, self._config.max_limit)
        return query.offset(offset).limit(limit)

    async def filter_by_query(
            self,
            filter_query: FilterQuery,
            session: AsyncSession,
            base_query: Select | None = None,
    ) -> FilterResult:
        """Фильтрация через FilterQuery объект"""
        # Базовый запрос
        query = base_query or select(self._model)

        query = self._apply_filter_groups(query, filter_query.groups)
        count_query = select(func.count()).select_from(query.subquery())

        query = self._apply_sorting(
            query, filter_query.order_by, filter_query.desc
        )
        query = self._apply_pagination(
            query, filter_query.offset, filter_query.limit
        )

        total_row = await session.exec(count_query)
        total = total_row.one()

        items_result = await session.exec(query)
        items = items_result.all()

        return FilterResult(items=items, total=total)

    async def filter_by_params(
            self,
            filter_params: FilterParams,
            session: AsyncSession,
            base_query: Select | None = None
    ) -> FilterResult:
        """Фильтрация через JSON параметры"""
        query = base_query or select(self._model)

        if filter_params.filters:
            simple_filters = []
            invalid_fields = []

            for key, value in filter_params.filters.items():
                if self._validate_field(key):
                    simple_filters.append(
                        Filter(name=key, op=Op.EQ, value=value))
                else:
                    invalid_fields.append(key)

            # Если есть недопустимые поля, выбрасываем ошибку
            if invalid_fields:
                raise ValueError(
                    f"'Недопустимые поля для фильтрации: {', '.join(invalid_fields)}'")

            if simple_filters:
                query = self._apply_filter_groups(
                    query, [FilterGroup(filters=simple_filters)]
                )

        # Применяем расширенные фильтры
        if filter_params.advanced_filters:
            advanced_groups = []

            for advanced_filter in filter_params.advanced_filters:
                if 'operator' in advanced_filter and 'items' in advanced_filter:
                    # Нормализация оператора группы
                    group_op_str = advanced_filter['operator'].lower()
                    group_op = 'and' if group_op_str in ['and',
                                                         'all'] else 'or'

                    filters = []
                    invalid_fields = []

                    for item in advanced_filter['items']:
                        if 'name' in item and 'value' in item:
                            field_name = item['name']

                            if not self._validate_field(field_name):
                                invalid_fields.append(field_name)
                                continue

                            # Получаем оператор (по умолчанию '=')
                            op_str = item.get('operator', '=')
                            filter_op = Op.from_string(op_str)

                            filters.append(Filter(
                                name=field_name,
                                op=filter_op,
                                value=item['value'],
                                type=item.get('data_type')
                            ))

                    # Если есть недопустимые поля, выбрасываем ошибку
                    if invalid_fields:
                        raise ValueError(
                            f"'Недопустимые поля для фильтрации: {', '.join(invalid_fields)}'")

                    if filters:
                        advanced_groups.append(
                            FilterGroup(op=group_op, filters=filters)
                        )

            if advanced_groups:
                query = self._apply_filter_groups(query, advanced_groups)

        # Подсчет общего количества
        count_query = select(func.count()).select_from(query.subquery())
        result = await session.exec(count_query)
        total = result.one()

        # Применяем сортировку
        query = self._apply_sorting(
            query, filter_params.order_by, filter_params.is_desc
        )

        # Применяем пагинацию и выполняем
        query = self._apply_pagination(
            query, filter_params.offset, filter_params.limit
        )
        result = await session.exec(query)
        items = result.all()

        return FilterResult(items=items, total=total)

    async def filter_by_url(
            self,
            url_params: dict[str, t.Any],
            session: AsyncSession,
            base_query: Select | None = None
    ) -> FilterResult:
        """Фильтрация по URL параметрам"""
        # Преобразуем URL параметры в FilterQuery
        filter_query = FilterQuery()

        # Пагинация и сортировка
        if 'offset' in url_params:
            filter_query.offset = int(url_params['offset'])
        if 'limit' in url_params:
            filter_query.limit = int(url_params['limit'])
        if 'order_by' in url_params:
            filter_query.order_by = url_params['order_by']
        if 'asc' in url_params:
            filter_query.desc = url_params['asc'] == '0'

        # Простые фильтры
        exclude_params = {'offset', 'limit', 'order_by', 'asc'}
        simple_filters = [
            Filter(name=key, op=Op.EQ, value=value)
            for key, value in url_params.items()
            if key not in exclude_params and self._validate_field(key)
        ]

        if simple_filters:
            filter_query.groups = [FilterGroup(filters=simple_filters)]

        return await self.filter_by_query(
            filter_query, session, base_query
        )

    async def quick_filter(
            self,
            session: AsyncSession,
            base_query: Select | None = None,
            **filters
    ) -> FilterResult:
        """Быстрая фильтрация по точным значениям"""
        filter_items = [
            Filter(name=key, op=Op.EQ, value=value)
            for key, value in filters.items()
            if self._validate_field(key)
        ]

        if not filter_items:
            filter_query = FilterQuery()
        else:
            filter_query = FilterQuery(
                groups=[FilterGroup(filters=filter_items)]
            )

        return await self.filter_by_query(
            filter_query, session, base_query
        )

    async def paginate(
            self,
            session: AsyncSession,
            page: int = 1,
            size: int | None = None,
            order_by: str | None = None,
            desc: bool = False,
            base_query: Select | None = None
    ) -> FilterResult:
        """Простая пагинация"""
        size = size or self._config.default_limit
        offset = (page - 1) * size

        filter_query = FilterQuery(
            offset=offset,
            limit=size,
            order_by=order_by,
            desc=desc
        )

        return await self.filter_by_query(
            filter_query, session, base_query
        )