import json
import uuid
from typing import Dict, Any
from pika import PlainCredentials, ConnectionParameters, BlockingConnection
from pika.spec import BasicProperties


class BrokerHelper:
    """Класс для работы с RabbitMQ брокером"""
    
    def __init__(self, host: str, port: int, credentials: PlainCredentials):
        self.connection_parameters = ConnectionParameters(
            host=host,
            port=port,
            credentials=credentials,
        )
        self._setup_queues()

    def _setup_queues(self):
        """Настраивает очереди с правильными параметрами"""
        try:
            connection = BlockingConnection(self.connection_parameters)
            channel = connection.channel()
            
            # Объявляем очереди с одинаковыми параметрами
            channel.queue_declare(queue='user_create', durable=False)
            channel.queue_declare(queue='user_delete', durable=False)
            channel.queue_declare(queue='user_responses', durable=False)
            
            connection.close()
            print(" Очереди брокера настроены")
        except Exception as e:
            print(f" Ошибка настройки очередей брокера: {e}")

    def user_create(self, username: str, email: str, password: str) -> str:
        """Отправка сообщения о создании пользователя"""
        user_data = {
            "username": username,
            "email": email,
            "password": password
        }
        return self._send_message("user_create", user_data)

    def user_delete(self, username: str) -> str:
        """Отправка сообщения об удалении пользователя"""
        return self._send_message("user_delete", {"username": username})

    def _send_message(self, queue: str, data: Dict[str, Any]) -> str:
        """Базовая отправка сообщения"""
        correlation_id = str(uuid.uuid4())
        
        try:
            with BlockingConnection(self.connection_parameters) as connection:
                with connection.channel() as channel:
                    # Используем пассивное объявление чтобы не менять параметры
                    try:
                        channel.queue_declare(queue=queue, passive=True)
                    except Exception:
                        # Если очереди нет, создаем с default параметрами
                        channel.queue_declare(queue=queue, durable=False)
                    
                    # Также для очереди ответов
                    try:
                        channel.queue_declare(queue='user_responses', passive=True)
                    except Exception:
                        channel.queue_declare(queue='user_responses', durable=False)
                    
                    channel.basic_publish(
                        exchange='',
                        routing_key=queue,
                        body=json.dumps(data),
                        properties=BasicProperties(
                            correlation_id=correlation_id,
                            reply_to='user_responses',
                        )
                    )
            
            print(f" Сообщение отправлено в {queue}, Correlation ID: {correlation_id}")
            return correlation_id
            
        except Exception as e:
            print(f" Ошибка отправки сообщения: {e}")
            raise