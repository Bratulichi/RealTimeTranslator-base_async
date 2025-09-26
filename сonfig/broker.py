
from base_module import Model
from sqlmodel import Field

class BrokerConnectionConfig(Model):
    """."""

    host: str = Field(default='rabbit')
    port: int = Field(default=5672)
    user: str = Field(default='admin')
    password: str = Field(default='admin')



class BrokerConsumerConfig(Model):
    """."""

    queue_name: str = Field(default='')
    error_timeout: int = Field(default=10)
    max_priority: int = Field(default=5)



class BrokerPublisherConfig(Model):
    """."""

    exchange: str = Field(default='')
    routing_key: str = Field(default='')
    reply_to: str = Field(default='')
    dlx_message_ttl_sec: int = Field(default=30)



class BrokerConfig(Model):
    """."""

    connection: BrokerConnectionConfig = Field(
        default=BrokerConnectionConfig
    )
    consumer: BrokerConsumerConfig = Field(default=BrokerConsumerConfig)
    publisher: BrokerPublisherConfig = Field(
        default=BrokerPublisherConfig
    )
