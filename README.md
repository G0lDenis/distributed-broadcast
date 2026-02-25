# Надёжное причинно-следственное широковещание

В рамках данной лабораторной работы необходимо реализовать:

1. **Reliable Broadcast** — надёжную рассылку сообщений всем узлам (включая отправителя) с доставкой ровно один раз при модели **crash-recovery**.
2. **Causal Broadcast** — причинный порядок доставки с помощью векторных часов.
3. **Orderer** — линейное расширение частичного порядка и выделение параллельных (конкурентных) сообщений.

Работа выполняется на Go с использованием фреймворка [Hive](https://github.com/nikitakosatka/hive).

---

## Контекст

- **Сеть**: надёжная (без потерь и дубликатов).
- **Модель узлов**: **crash-recovery** — узлы могут падать и восстанавливаться.

---

## Что нужно реализовать

Необходимо реализовать:

```go
func NewReliableCausalBroadcastNode(id string, allNodeIDs []string) BroadcastNode
```

и интерфейс:

```go
type BroadcastNode interface {
    hive.Node
    Broadcast(payload interface{}) error
    DeliveredMessages() []hive.Message
}
```

а также метод `Orderer.Order`:

```go
func (o *Orderer) Order(msgs ...hive.Message) (ordered []string, parallel [][]string)
```
---

## Требования

### 1. Reliable Broadcast (flooding)

- При первом получении сообщения — пересылать его всем остальным узлам (flood).
- Дедупликация — каждое сообщение доставляется ровно один раз.
- Периодический re-flood доставленных сообщений всем узлам, чтобы после crash-recovery все корректные узлы в итоге получили одно и то же множество сообщений.

### 2. Causal Broadcast

- Каждое сообщение несёт логические часы в метаданных.
- Упорядочивание сообщений в причинно-следственном порядке.

### 3. Orderer

Метод `Order` должен:

- определить частичный порядок сообщений
- построить его линейное расширение
- вернуть группы параллельных сообщений

---

## Полезные материалы

**Reliable Broadcast:**

- Bracha, G. (1987). Asynchronous Byzantine Agreement Protocols.

**Causal Broadcast:**

- Schiper, A., Eggli, J., & Sandoz, A. (1989). A new algorithm to implement causal ordering.

---

## Тестирование

```bash
go test ./...
```
