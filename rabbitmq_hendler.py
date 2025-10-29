import json
import time
import threading
from typing import Dict, Any
from pika import BlockingConnection, ConnectionParameters, PlainCredentials
from pika.exceptions import AMQPConnectionError


class ResponseHandler:
    """Обработчик ответов от RabbitMQ"""
    
    def __init__(self, connection_parameters: ConnectionParameters):
        self.connection_parameters = connection_parameters
        self.responses = {}
        self._start_background_listener()

    def _start_background_listener(self):
        """Запускает фоновое прослушивание ответов"""
        def listener():
            while True:
                try:
                    connection = BlockingConnection(self.connection_parameters)
                    channel = connection.channel()
                    
                    # Объявляем очередь для ответов
                    channel.queue_declare(queue='user_responses', durable=False)
                    print(" Фоновый слушатель: очередь user_responses готова")

                    def callback(ch, method, properties, body):
                        try:
                            response = json.loads(body)
                            correlation_id = response.get('correlation_id')
                            
                            if correlation_id:
                                self.responses[correlation_id] = response
                                print(f" ФОН: Получен ответ для {correlation_id}")
                            
                            ch.basic_ack(delivery_tag=method.delivery_tag)
                            
                        except Exception as e:
                            print(f" ФОН: Ошибка обработки: {e}")
                            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                    
                    channel.basic_consume(
                        queue='user_responses',
                        on_message_callback=callback,
                        auto_ack=False
                    )
                    
                    print(" Фоновый слушатель ответов запущен и ожидает сообщения...")
                    channel.start_consuming()
                    
                except AMQPConnectionError as e:
                    print(f" Ошибка подключения в фоновом слушателе: {e}")
                    time.sleep(5)
                except Exception as e:
                    print(f" Ошибка в фоновом слушателе: {e}")
                    time.sleep(5)

        # Запускаем в отдельном потоке
        thread = threading.Thread(target=listener, daemon=True)
        thread.start()

    def wait_for_response(self, correlation_id: str, timeout: int = 10) -> Dict[str, Any]:
        """Ожидает ответ с указанным correlation_id"""
        print(f" Ожидание ответа для {correlation_id} (таймаут: {timeout}сек)")
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            if correlation_id in self.responses:
                response = self.responses.pop(correlation_id)
                print(f" Ответ найден для {correlation_id}")
                return response
            
            time.sleep(0.1)  # Небольшая пауза между проверками
        
        # Таймаут
        timeout_response = {
            "correlation_id": correlation_id,
            "status": "error",
            "error": f"Timeout waiting for response ({timeout}s)",
            "data": {}
        }
        print(f" Таймаут для {correlation_id}")
        return timeout_response

    def get_pending_responses(self):
        """Возвращает количество ожидающих ответов"""
        return len(self.responses)