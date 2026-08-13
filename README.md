# Домашнее задание: Инфраструктурные паттерны (RESTful CRUD в Kubernetes)

Данный проект реализует простейший RESTful CRUD микросервис по созданию, удалению, просмотру и обновлению пользователей. Приложение написано на Python (FastAPI) и подключено к базе данных PostgreSQL.

## Инструкция по запуску

### 1. Подготовка окружения
Перед запуском требуется убедиться, что установлен и запущен `minikube` (или альтернативный локальный кластер Kubernetes), а также включен Ingress-контроллер:
```bash
minikube addons enable ingress
```

Добавьте тестовый домен в файл `/etc/hosts` (или `C:\Windows\System32\drivers\etc\hosts` для Windows):
```text
127.0.0.1 arch.homework
```

### 2. Создание Namespace и установка базы данных через Helm
Конфигурация параметров БД передается через кастомный файл настроек `database/db-values.yaml`.

```bash
# Создаем namespace для изоляции домашнего задания
kubectl create namespace homework

# Добавляем официальный репозиторий Bitnami для PostgreSQL
helm repo add bitnami https://bitnami.com
helm repo update

# Устанавливаем PostgreSQL с доступами в созданный namespace
helm install postgres bitnami/postgresql -n homework -f database/db-values.yaml
```

### 3. Запуск манифестов приложения
Применить манифесты Kubernetes в правильном порядке. Конфигурация приложения хранится в ConfigMap, доступы к БД — в Secrets.

```bash
# 1. Применяем ConfigMap с адресом и портом БД
kubectl apply -f k8s/01-configmap.yaml

# 2. Применяем Secret с логином и паролем (в base64)
kubectl apply -f k8s/02-secret.yaml

# 3. Запускаем Deployment приложения 
kubectl apply -f k8s/03-deployment.yaml

# 4. Создаем Service для доступа к подам
kubectl apply -f k8s/04-service.yaml

# 5. Настраиваем Ingress-маршрутизацию на домен arch.homework/
kubectl apply -f k8s/05-ingress.yaml
```

Убедиться, что все поды перешли в статус `Running`:
```bash
kubectl get pods -n homework
```

### 4. Проверка корректности работы (Newman / Postman)
Для автоматического тестирования CRUD-методов используется встроенная Postman-коллекция, отправляющая запросы на базовый URL `arch.homework`.

Установить `newman` (требуется Node.js) и запустить тесты:
```bash
npm install -g newman
newman run postman/postman_collection.json
```
