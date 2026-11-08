# Pipelines : consumers Kafka directs à la place de Kafka Streams

Dépend de `sitewhere-microservice-core` **3.0.11f** (`DirectKafkaConsumer` : `assign()` des partitions sans consumer group, offsets `latest`, pas de commit d'offsets).

## Pipelines passés de `KafkaStreamPipeline` à `DirectKafkaConsumer`

Les topics d'entrée et de sortie ne changent pas. Chaque pipeline remplace `buildStreams()` / `getPipelineName()` par `attemptToProcess()`, qui désérialise chaque record et appelle le même mapper/processor qu'avant. Les interfaces SPI passent de `IKafkaStreamPipeline` à `IMicroserviceKafkaConsumer`.

### `buildStreams(StreamsBuilder builder)` (ancien modèle, `KafkaStreamPipeline`)

Méthode abstraite appelée une seule fois, dans `KafkaStreamPipeline.initialize()`. Elle ne traite aucun message : elle décrit une topologie Kafka Streams (`builder.stream(...)`, `map`, `branch`, `flatMapValues`, `to(...)`), avec les Serdes de clé et de valeur passés via `Consumed.with(...)` / `Produced.with(...)`. La topologie est ensuite compilée en `KafkaStreams`, puis démarrée dans `start()` et fermée dans `stop()`.

Le moteur Kafka Streams gère alors seul le polling, la désérialisation, le routage vers les topics de sortie et le commit des offsets, dans un consumer group dont l'`application.id` est construit à partir de `getPipelineName()`. Les erreurs de désérialisation sont journalisées puis ignorées (`LogAndContinueExceptionHandler`).

### `attemptToProcess(TopicPartition topicPartition, List<ConsumerRecord<String, byte[]>> records)` (nouveau modèle, `DirectKafkaConsumer`)

Méthode abstraite appelée à chaque `poll()`, une fois par partition, avec les records typés `ConsumerRecord<String, byte[]>`.

Dans Kafka, la clé et la valeur sont toujours stockées sous forme d'octets. Avant, les Serdes passés à `Consumed.with(...)` faisaient la conversion : `Serdes.String()` ou `Serdes.UUID()` pour la clé. Or `Serdes.UUID()` (`UUIDSerializer`/`UUIDDeserializer`) encode simplement `uuid.toString()` en UTF-8, soit 36 octets. Désormais, le `KafkaConsumer` du core décode la clé avec `StringDeserializer` et laisse la valeur brute (`ByteArrayDeserializer`). Pour une clé écrite par `Serdes.UUID()`, on récupère donc la chaîne du UUID, que `UUID.fromString` reconvertit. Les octets sur le topic sont identiques dans les deux cas.

Déroulement :

1. le thread de `MicroserviceKafkaConsumer` fait un `assign()` de toutes les partitions des topics sources, puis enchaîne les `poll()` ;
2. pour chaque partition présente dans le résultat, il appelle `process()`, qui délègue à `attemptToProcess()` ;
3. dans chaque pipeline, `attemptToProcess()` boucle sur les records, désérialise la valeur avec le même Serde SiteWhere qu'avant (`SiteWhereSerdes.forXxx().deserializer()`), reconvertit en `UUID` la clé déjà décodée en `String` quand le mapper attend un UUID (`UUID.fromString(record.key())`), puis appelle directement le mapper/processor.

Une `SiteWhereException` ou toute autre exception levée par `attemptToProcess()` est journalisée par `DirectKafkaConsumer.process()`. Comme la boucle est dans `attemptToProcess()`, une exception sur un record interrompt le traitement des records suivants du même lot pour cette partition. Ces records ne sont pas rejoués, puisqu'aucun offset n'est commité.

La différence de fond : `buildStreams()` déclare un traitement qu'exécute Kafka Streams, tandis que `attemptToProcess()` est ce traitement, exécuté de façon impérative et synchrone. Les envois vers d'autres topics (cas de `DecodedEventsPipeline`) doivent donc passer par des producers explicites.

| Service | Pipeline | Traitement par record |
|---|---|---|
| inbound-processing | `DecodedEventsPipeline` | voir ci-dessous |
| event-management | `EventPersistencePipeline` | `EventPersistenceMapper.apply(UUID.fromString(key), preprocessedPayload)` |
| device-registration | `RegistrationEventsPipeline` | `RegistrationEventsProcessor.process(key, registrationPayload)` |
| device-registration | `UnregisteredEventsPipeline` | `UnregisteredEventsProcessor.process(key, decodedPayload)` |
| command-delivery | `EnrichedCommandInvocationsPipeline` | `CommandInvocationsProcessor.process(UUID.fromString(key), processedPayload)` |

### `DecodedEventsPipeline`

Les branches KStream sont remplacées par un traitement séquentiel, record par record :

1. recherche du device (`DeviceLookupMapper`) ;
2. device inconnu : le payload décodé d'origine est envoyé sur le topic **unregistered device events** ;
3. device connu : recherche des assignments, puis un payload prétraité par assignment est envoyé sur le topic **inbound events**.

L'envoi passe par deux producers internes (`TopicProducer`, clé `String`, un par topic), initialisés, démarrés et arrêtés avec le pipeline. Les clés UUID sont envoyées sous forme de chaîne, soit les mêmes octets qu'avec le sérialiseur UUID d'avant.

## Outbound connectors

`KafkaOutboundConnectorHost` : suppression du `commitAsync()` après chaque batch, puisqu'il n'y a plus de consumer group.

## Build / images Docker

- `build.gradle` : `sitewhere.microservice.api.version` passe à `3.0.11f`, et le namespace Docker des tags (`dockerImage`, `dockerPushImage`) n'est plus fixé à `grosinosky` mais lu dans la propriété `dockerNamespace`.
- `gradle.properties` : `dockerNamespace=grosinosky` par défaut.
- `justfile` (nouveau) : `publish-core` (publication du core en Maven local), `build`, `build-changed`, `build-debug`, `push`, `push-changed`. Il pousse vers `docker.io/gaetanplisson` et force l'encodage UTF-8 de javac.
