# Домашнее задание: Метрики, Prometheus и Grafana

Данный проект расширяет предыдущий RESTful CRUD микросервис, добавляя сбор ключевых метрик в формате Prometheus и их визуализацию в Grafana.

## Добавленные метрики и PromQL-запросы

В Grafana настроены два блока графиков (с разбиением по API-методам для приложения и путям для Ingress):

1. **RPS (Количество запросов в секунду)**
   * Приложение: `sum(rate(http_requests_total{namespace="homework"}[1m])) by (method, handler)`
   * Ingress: `sum(rate(nginx_ingress_controller_requests{namespace="homework"}[1m])) by (method, path)`

2. **Latency (Время ответа с квантилями p50, p95, p99, max)**
   * Приложение (Квантиль 0.95): `histogram_quantile(0.95, sum(rate(http_request_duration_seconds_bucket{namespace="homework"}[1m])) by (le, method, handler))`
   * Ingress (Квантиль 0.95): `histogram_quantile(0.95, sum(rate(nginx_ingress_controller_request_duration_seconds_bucket{namespace="homework"}[1m])) by (le, method, path))`
   * *Для max используется функция `max(...)` по соответствующим метрикам длительности запроса.*

3. **Error Rate (Количество 500-х ответов)**
   * Приложение: `sum(rate(http_requests_total{namespace="homework", status=~"5.."}[1m])) by (method, handler)`
   * Ingress: `sum(rate(nginx_ingress_controller_requests{namespace="homework", status=~"5.."}[1m])) by (method, path)`

## Настройка алертинга (Alerting) в Grafana

В Grafana настроены два правила оповещения (Alert Rules):
1. **High Error Rate Alert**: Срабатывает, если количество 5xx ошибок превышает 5% от общего числа запросов в течение 2 минут (`Error Rate / Total RPS > 0.05`).
2. **High Latency Alert**: Срабатывает, если 95-й процентиль времени ответа (p95 Latency) превышает 1.5 секунды в течение 3 минут.

## Инструкция по запуску и импорту дашборда

1. Примените обновленные манифесты:
   ```bash
   kubectl apply -f manifests/
   ```
2. Файл конфигурации дашборда находится в директории `grafana/dashboard.json`. Чтобы импортировать его в вашу Grafana:
   * Перейдите в меню **Dashboards** -> **New** -> **Import**.
   * Загрузите файл `dashboard.json` или вставьте его содержимое в текстовое поле.
   * Выберите ваш Prometheus в качестве источника данных (Data Source) и нажмите **Import**.
