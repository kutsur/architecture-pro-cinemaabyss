## Изучите [README.md](README.md) файл и структуру проекта.

## Задание 1

**Диаграмма контейнеров (Containers)**

- [C4 Container Diagram (PlantUML)](docs/architecture-c4-container-diagram.puml)

## Задание 2

### 1. Proxy

### 2. Kafka

- [Kafka Topics](screenshots/kafka-topics.png)
- [Kafka Consumers](screenshots/kafka-consumers.png)
- [Tests](screenshots/tests.png)

## Задание 3

Команда начала переезд в Kubernetes для лучшего масштабирования и повышения надежности.
Вам, как архитектору осталось самое сложное:

- реализовать CI/CD для сборки прокси сервиса
- реализовать необходимые конфигурационные файлы для переключения трафика.

### CI/CD

- [CI / CD](screenshots/ci-cd-api-tests.png)
- [Pipelines](screenshots/pipelines.png)

### Proxy в Kubernetes

- [Cinema Response](screenshots/cinema-response.png)
- [Log event](screenshots/events.png)
- [Kubernetes pods](screenshots/kuber-pods.png)

## Задание 4

Выполнено, команды для старта

```bash
kubectl delete all --all -n cinemaabyss
```

```bash
helm install cinemaabyss src/kubernetes/helm --namespace cinemaabyss --create-namespace
```

```bash
helm status cinemaabyss -n cinemaabyss
```

```bash
kubectl -n cinemaabyss get all
```

- [helm](screenshots/helm.png)

# Задание 5

- [Circuit statistic](screenshots/circuit-1.png)
- [Circuit statistic](screenshots/circuit-2.png)
