import json
from typing import Dict, Any

from pika import BlockingConnection, ConnectionParameters

from rabbit_client import rmq


class ResponseHandler:
    """Обработчик ответов от сервера"""

    def __init__(self, connection_parameters: ConnectionParameters):
        self.connection_parameters = connection_parameters
        self.pending_requests = {}  # для отслеживания ожидающих ответов

    def wait_for_response(self, correlation_id: str, timeout: int = 10) -> Dict[str, Any]:
        """Ожидание ответа по correlation_id"""
        import time

        response = None
        start_time = time.time()

        def response_callback(ch, method, properties, body):
            nonlocal response
            response_data = json.loads(body)
            if response_data['correlation_id'] == correlation_id:
                response = response_data
                ch.basic_ack(delivery_tag=method.delivery_tag)
                ch.stop_consuming()

        connection = BlockingConnection(self.connection_parameters)
        channel = connection.channel()

        channel.basic_consume(
            queue='user_responses',
            on_message_callback=response_callback,
            auto_ack=False
        )

        # Ждем ответ в течение timeout секунд
        while time.time() - start_time < timeout and response is None:
            connection.process_data_events(time_limit=1)

        connection.close()

        if response is None:
            return {
                "correlation_id": correlation_id,
                "status": "timeout",
                "data": None,
                "error": "Response timeout"
            }

        return response


# Пример использования клиента с ожиданием ответа
def create_user_with_response(username: str, email: str, password: str):
    # Отправляем запрос
    correlation_id = rmq.user_create(username, email, password)

    # Ждем ответ
    response_handler = ResponseHandler(rmq.rmq_parameters)
    response = response_handler.wait_for_response(correlation_id)

    if response['status'] == 'ok':
        print(f"User created successfully: {response['data']}")
    else:
        print(f"Error creating user: {response['error']}")

    return response

