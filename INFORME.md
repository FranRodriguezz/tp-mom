# TP1 - Sistemas Distribuidos I (75.74) - Informe

**Alumno**: Franco Ezequiel Rodríguez

**Padrón**: 102815

**Facultad**: FIUBA — Facultad de Ingeniería, Universidad de Buenos Aires

**Materia**: Sistemas Distribuidos I

**Cuatrimestre**: 2do 2026

---

## Diseño general

Se optó por implementar la interfaz en **Python**, utilizando la librería
`pika` como cliente de RabbitMQ. Se implementaron las dos clases requeridas
por la interfaz (`MessageMiddlewareQueue` y `MessageMiddlewareExchange`) en
`middleware_rabbitmq.py`, cada una modelando un patrón de comunicación
distinto sobre el mismo broker.

| | `MessageMiddlewareQueueRabbitMQ` | `MessageMiddlewareExchangeRabbitMQ` |
|---|---|---|
| Patrón | Work Queue (competing consumers) | Publish/Subscribe por routing key |
| Cola | Única, con nombre fijo (`queue_name`), compartida por todas las instancias | Propia y anónima por instancia, exclusiva |
| Exchange | Default de RabbitMQ (`""`) | Declarado explícitamente, tipo `direct` |
| Binding | No aplica (ruteo directo por nombre de cola) | Una cola por instancia, bindeada a cada routing key recibida |

## `MessageMiddlewareQueueRabbitMQ`

Modela el patrón de **Work Queue**: múltiples productores publican a una
misma cola, y múltiples consumidores compiten por sus mensajes (cada mensaje
es procesado por exactamente uno de ellos). Se utiliza el exchange por
defecto de RabbitMQ (nombre `""`), publicando con `routing_key` igual al
nombre de la cola, que es la forma estándar de entregar un mensaje
directamente a una cola sin declarar un exchange propio.

La cola se declara con `durable=True`, para que sobreviva a un reinicio del
broker. Los mensajes se publican con `delivery_mode=2` (persistente), de
forma consistente con esa decisión.

## `MessageMiddlewareExchangeRabbitMQ`

Modela un esquema de **publish/subscribe por routing key**. Se declara un
exchange propio de tipo `direct`, y cada instancia crea su propia cola
anónima y exclusiva (`queue_declare(queue='', exclusive=True)`), bindeada a
cada una de las routing keys recibidas por parámetro.

**Por qué `direct` y no `fanout`**: un exchange `fanout` entrega a todas las
colas bindeadas sin considerar routing key, lo cual hubiese simplificado el
caso de broadcast, pero no permite el caso de mensajería dirigida (un
consumer que solo quiere los mensajes de una routing key puntual, sin
recibir las de otros). Un exchange `direct` cubre ambos casos: el
comportamiento "broadcast" se logra simplemente con que varias colas
distintas compartan el mismo binding (misma routing key), sin necesidad de
un tipo de exchange separado.

**Por qué la cola es exclusiva y no durable**: al ser anónima y propia de
una única instancia/conexión, no tiene sentido que sobreviva a un reinicio
del broker — RabbitMQ la elimina automáticamente en cuanto se cierra la
conexión que la creó. Combinar `exclusive=True` con `durable=True` sería
semánticamente inconsistente (una cola pensada para sobrevivir reinicios que
de todas formas desaparece al cerrar la conexión).

Para el envío (`send`), la clase asume que fue instanciada con una lista de
una única routing key (uso como productor) y publica usando esa clave. Para
el consumo, se bindean todas las routing keys de la lista recibida, sin
límite de cantidad.

## Manejo de conexión y errores

Ambas clases envuelven cada llamada a `pika` (conexión, creación de channel,
declaración de colas/exchange, bindings, publish, consume) en bloques
`try/except`, traduciendo las excepciones nativas de `pika` a las excepciones
propias de la interfaz (`MessageMiddlewareDisconnectedError`,
`MessageMiddlewareMessageError`, `MessageMiddlewareCloseError`), de forma que
quien utilice esta interfaz no necesite conocer que la implementación
subyacente usa RabbitMQ — cumpliendo con el requerimiento no funcional de
respetar la abstracción del middleware y encapsular sus errores.

En `send`/`start_consuming` se distingue explícitamente entre errores de
conexión (`pika.exceptions.AMQPConnectionError`, mapeado a
`MessageMiddlewareDisconnectedError`) y cualquier otro error interno
(mapeado a `MessageMiddlewareMessageError`), siguiendo la distinción que
plantea el docstring de la interfaz. `close`/`stop_consuming` solo elevan
`MessageMiddlewareCloseError`/`MessageMiddlewareDisconnectedError`
respectivamente, según lo que documenta cada método de la interfaz.

`close()` verifica `is_open` de forma independiente para el channel y la
conexión antes de cerrarlos, de forma que sea idempotente (llamarlo más de
una vez, o sobre una conexión ya cerrada por otra causa, no genera error).

## Ack/Nack manual

Se optó por `auto_ack=False` en ambas clases: el ack/nack de cada mensaje se
delega al usuario de la interfaz a través de dos funciones (`ack`, `nack`)
que se le proveen en el callback de consumo, tal como lo especifica la
interfaz. Internamente, se implementa mediante un wrapper que adapta la
firma de callback que exige `pika`
(`channel, method, properties, body`) a la firma que expone la interfaz
(`message, ack, nack`); las funciones `ack`/`nack` capturan por clausura el
`channel` y el `delivery_tag` del mensaje puntual que se está procesando.

`nack` se implementa con `requeue=True`, priorizando no perder mensajes ante
un fallo de procesamiento por sobre el riesgo de reprocesamiento.

---

## Fixes de última hora (validación contra `make test`)

Los 19 tests provistos por la cátedra (13 de `test_queue.py`, 6 de
`test_exchange.py`) pasaron sin necesidad de ajustes adicionales al diseño
descrito en las secciones anteriores.