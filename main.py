import json
import time
import threading
from typing import Dict, Any

from config import settings
from pika import PlainCredentials, BlockingConnection

from rabbitmq_hendler import ResponseHandler
from rabbit_client import BrokerHelper


class TestClient:
    def __init__(self):
        self.broker = BrokerHelper(
            host=settings.rmq.host,
            port=settings.rmq.port,
            credentials=PlainCredentials(
                username=settings.rmq.username,
                password=settings.rmq.password,
            ),
        )
        self.response_handler = ResponseHandler(self.broker.rmq_parameters)

    def print_separator(self, title: str):
        """Печатает разделитель с заголовком"""
        print("\n" + "=" * 60)
        print(f" {title} ")
        print("=" * 60)

    def test_user_create(self, username: str, email: str, password: str) -> Dict[str, Any]:
        """Тестирует создание пользователя с ожиданием ответа"""
        self.print_separator("ТЕСТ СОЗДАНИЯ ПОЛЬЗОВАТЕЛЯ")
        print(f"Отправка запроса на создание пользователя:")
        print(f"  Username: {username}")
        print(f"  Email: {email}")
        print(f"  Password: {password}")

        # Отправляем запрос
        print("\n📤 Отправка сообщения в очередь...")
        correlation_id = self.broker.user_create(username, email, password)
        print(f"✅ Сообщение отправлено. Correlation ID: {correlation_id}")

        # Ждем ответ
        print("\n⏳ Ожидание ответа от сервера...")
        response = self.response_handler.wait_for_response(correlation_id, timeout=15)

        # Выводим результат
        print("\n📥 Получен ответ от сервера:")
        print(f"  Correlation ID: {response.get('correlation_id')}")
        print(f"  Status: {response.get('status')}")
        print(f"  Data: {json.dumps(response.get('data'), indent=2)}")
        print(f"  Error: {response.get('error')}")

        if response['status'] == 'ok':
            print("🎉 Пользователь успешно создан!")
        else:
            print("❌ Ошибка при создании пользователя!")

        return response

    def test_user_delete(self, username: str) -> Dict[str, Any]:
        """Тестирует удаление пользователя с ожиданием ответа"""
        self.print_separator("ТЕСТ УДАЛЕНИЯ ПОЛЬЗОВАТЕЛЯ")
        print(f"Отправка запроса на удаление пользователя:")
        print(f"  Username: {username}")

        # Отправляем запрос
        print("\n📤 Отправка сообщения в очередь...")
        correlation_id = self.broker.user_delete(username)
        print(f"✅ Сообщение отправлено. Correlation ID: {correlation_id}")

        # Ждем ответ
        print("\n⏳ Ожидание ответа от сервера...")
        response = self.response_handler.wait_for_response(correlation_id, timeout=15)

        # Выводим результат
        print("\n📥 Получен ответ от сервера:")
        print(f"  Correlation ID: {response.get('correlation_id')}")
        print(f"  Status: {response.get('status')}")
        print(f"  Data: {json.dumps(response.get('data'), indent=2)}")
        print(f"  Error: {response.get('error')}")

        if response['status'] == 'ok':
            print("🎉 Пользователь успешно удален!")
        else:
            print("❌ Ошибка при удалении пользователя!")

        return response

    def test_async_operations(self):
        """Тестирует асинхронные операции без ожидания ответа"""
        self.print_separator("ТЕСТ АСИНХРОННЫХ ОПЕРАЦИЙ")

        print("Отправка нескольких запросов без ожидания ответов...")

        # Создаем несколько пользователей
        users = [
            {"username": "async_user1", "email": "async1@test.com", "password": "pass123"},
            {"username": "async_user2", "email": "async2@test.com", "password": "pass456"},
            {"username": "async_user3", "email": "async3@test.com", "password": "pass789"},
        ]

        correlation_ids = []

        for user in users:
            correlation_id = self.broker.user_create(
                user["username"],
                user["email"],
                user["password"]
            )
            correlation_ids.append(correlation_id)
            print(f"📤 Отправлен запрос для {user['username']}, Correlation ID: {correlation_id}")
            time.sleep(0.5)  # Небольшая задержка между запросами

        print(f"\n✅ Всего отправлено {len(correlation_ids)} запросов")

        # Теперь ждем ответы на все запросы
        print("\n⏳ Ожидание ответов на все запросы...")

        for i, correlation_id in enumerate(correlation_ids):
            print(f"\n🔍 Проверка ответа для запроса {i + 1}...")
            response = self.response_handler.wait_for_response(correlation_id, timeout=10)

            print(f"📥 Ответ {i + 1}:")
            print(f"  Status: {response.get('status')}")
            print(f"  Username: {response.get('data', {}).get('username', 'N/A')}")
            print(f"  Error: {response.get('error')}")

    def test_error_scenarios(self):
        """Тестирование сценариев с ошибками"""
        self.print_separator("ТЕСТ СЦЕНАРИЕВ С ОШИБКАМИ")

        print("1. Тест таймаута (несуществующий correlation_id)...")
        response = self.response_handler.wait_for_response("non_existent_id", timeout=3)
        print(f"Результат: {response['status']} - {response['error']}")

        print("\n2. Тест дублирования пользователя...")
        # Сначала создаем пользователя
        self.test_user_create("duplicate_user", "duplicate@test.com", "password123")

        print("\n3. Попытка создать пользователя с тем же username...")
        response = self.test_user_create("duplicate_user", "another@test.com", "password456")

        # Очистка
        self.test_user_delete("duplicate_user")

    def start_response_listener(self):
        """Запускает фоновый слушатель ответов"""

        def listen_for_responses():
            connection = BlockingConnection(self.broker.rmq_parameters)
            channel = connection.channel()

            def callback(ch, method, properties, body):
                response = json.loads(body)
                print(f"\n🎯 ФОН: Получен ответ:")
                print(f"   Correlation ID: {response.get('correlation_id')}")
                print(f"   Status: {response.get('status')}")
                print(f"   Data: {json.dumps(response.get('data'), indent=4)}")
                if response.get('error'):
                    print(f"   Error: {response.get('error')}")
                ch.basic_ack(delivery_tag=method.delivery_tag)

            channel.basic_consume(
                queue='user_responses',
                on_message_callback=callback,
                auto_ack=False
            )

            print("👂 Фоновый слушатель ответов запущен...")
            channel.start_consuming()

        # Запускаем в отдельном потоке
        listener_thread = threading.Thread(target=listen_for_responses, daemon=True)
        listener_thread.start()
        return listener_thread


def main():
    """Основная функция тестирования"""
    print("🚀 ЗАПУСК ТЕСТОВОГО КЛИЕНТА RABBITMQ")
    print("Настройки подключения:")
    print(f"  Host: {settings.rmq.host}")
    print(f"  Port: {settings.rmq.port}")
    print(f"  Username: {settings.rmq.username}")

    client = TestClient()

    # Запускаем фоновый слушатель
    print("\n🔄 Запуск фонового слушателя ответов...")
    client.start_response_listener()
    time.sleep(2)  # Даем время слушателю запуститься

    try:
        # Тест 1: Простое создание пользователя
        client.test_user_create("test_user_1", "test1@example.com", "secure_password_123")
        time.sleep(2)

        # Тест 2: Удаление пользователя
        client.test_user_delete("test_user_1")
        time.sleep(2)

        # Тест 3: Асинхронные операции
        client.test_async_operations()
        time.sleep(2)

        # Тест 4: Сценарии с ошибками
        client.test_error_scenarios()
        time.sleep(2)

        # Тест 5: Множественные операции
        client.print_separator("ТЕСТ МНОЖЕСТВЕННЫХ ОПЕРАЦИЙ")

        users_to_test = [
            {"username": "john_doe", "email": "john@example.com", "password": "johnpass"},
            {"username": "jane_smith", "email": "jane@example.com", "password": "janepass"},
            {"username": "bob_wilson", "email": "bob@example.com", "password": "bobpass"},
        ]

        created_users = []

        # Создаем пользователей
        for user in users_to_test:
            response = client.test_user_create(user["username"], user["email"], user["password"])
            if response['status'] == 'ok':
                created_users.append(user["username"])
            time.sleep(1)

        # Удаляем созданных пользователей
        for username in created_users:
            client.test_user_delete(username)
            time.sleep(1)

        print("\n" + "🎊" * 20)
        print("ВСЕ ТЕСТЫ ЗАВЕРШЕНЫ!")
        print("🎊" * 20)

    except KeyboardInterrupt:
        print("\n\n⏹️ Тестирование прервано пользователем")
    except Exception as e:
        print(f"\n\n❌ Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()

    print("\nДля выхода нажмите Ctrl+C...")


if __name__ == "__main__":
    main()