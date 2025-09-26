from base_module import Model


class BaseOrmModel(Model):
    """."""

    __name__: str

    # Generate __tablename__ automatically
    @classmethod
    @property
    def __tablename__(cls) -> str:
        return cls.__name__.lower()
