# Запуск приложения
# Ссоздайте .env
```
RMQ__USERNAME=user
RMQ__PASSWORD=123
RMQ__HOST=localhost
RMQ__PORT=5672
```
# Запустите docker-compose или просто соберите приложение
 - docker compose
 ```
docker compose up -d --build
 ```
 - build
```bash
python -m venv venv

venv/Scripts/activate #(win)
source venv/bin/activate #(Linux\Mac) 

pip install -r requirements.txt

python3.* main.py
```
