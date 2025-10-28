import json

from pika import PlainCredentials, ConnectionParameters, BlockingConnection


def confirm_handler(method_frame) -> str:
    if method_frame.method.NAME == 'Basic.Ack':
        return "Сообщение успешно доставлено в очередь"
    elif method_frame.method.NAME == 'Basic.Nack':
        return "Сообщение не было доставлено в очередь"


class BrokerHelper:
    def __init__(
            self,
            host: str,
            port: int,
            credentials: PlainCredentials,
    ) -> None:
        self.rmq_parameters = ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials
        )
        with BlockingConnection(self.rmq_parameters) as connection:
            with connection.channel() as channel:
                channel.queue_declare("user")
        # self.rmq_channel.queue_declare("manga_classic")

    def user_create(self, operation: str, username: str, email: str, password: str):
        with BlockingConnection(self.rmq_parameters) as connection:
            with connection.channel() as channel:
                message = json.dumps({
                        'operation': operation,
                        'username': username,
                        'email': email,
                        'password':password,
                    })
                channel.basic_publish(
                    exchange='',
                    routing_key="user_create",
                    body=message)
                result = channel.add_on_return_callback(confirm_handler)
                return result

    def user_profile_delete(self, operation: str, username: str):
        with BlockingConnection(self.rmq_parameters) as connection:
            with connection.channel() as channel:
                message = json.dumps({
                        'operation': operation,
                        'username': username,
                    })
                channel.basic_publish(
                    exchange='',
                    routing_key="user_delete",
                    body=message)
                result = channel.add_on_return_callback(confirm_handler)
                return result


rmq = BrokerHelper(
    host=settings.rmq.host,
    port=settings.rmq.port,
    credentials=PlainCredentials(
        username=settings.rmq.username,
        password=settings.rmq.password,
    ),
)
