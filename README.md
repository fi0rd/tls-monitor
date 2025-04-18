# tls-monitor

Сервис мониторит срок действия TLS сертификатов и отправляет уведомления в Grafana OnCall в случае приближения истечения срока их действия. Непосредственно мониторинг сертификатов осуществляет Prometheus Blackbox Exporter.
Данные о таргет URL'ах для Blackbox'а забираются из внешних API с помощью экспортеров (python/golnag). Blackbox Exporter: возвращает результат опроса URL'а сервиса для метрики "probe_ssl_earliest_cert_expiry"

## Основные функции
- Мониторинг сертификатов: проверка срока действия сертификатов для сервисов в Облаке.
- Интеграция с Grafana OnCall: отправка алертов в Grafana OnCall при достижении заданного периода до истечения сертификата (через вебхук в конфиге Prometheus Alermanager).
- Автоматизация: использование скриптов и экспортеров для автоматического сбора таргетов для использования в мониторинге.

## Описание работы
Сервис запускает по расписанию (systemd timers) следующие скрипты для генерации файла с таргетами для Blackbox Exporter'а:
- targets_exporter.py - сбор таргет URL'ов
- targets_fetcher.go - обогащения данными из внешнего API
- blackbox_config.py - генерация конечного конфига для Blackbox'а
