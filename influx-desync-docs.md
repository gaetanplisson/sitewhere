# Changements : écriture unique InfluxDB et retour par subscription (branche `desync-influx`)

## Flux avant

Un événement arrive par le topic **Kafka inbound**, traverse le pipeline et arrive
au service *event-management*. La persistance est **garantie** : l'écriture dans
InfluxDB est bien effectuée, et l'event est ensuite envoyé sur **Kafka outbound**
via [`KafkaEventPersistenceTriggers`](service-event-management/src/main/java/com/sitewhere/event/kafka/KafkaEventPersistenceTriggers.java) :

```
Kafka inbound ──> event-management
                    ├─> écriture dans InfluxDB (garantie)
                    └─> envoi sur Kafka outbound (par le service, juste après)
```

Autrement dit, le service effectue lui-même les deux envois : l'event part à la
fois dans InfluxDB et sur Kafka outbound, le push outbound étant directement
imbriqué dans le chemin de persistance.

Le chemin concret dans le code commis (état de la branche avant modifications) :

```
EventPersistencePipeline (DirectKafkaConsumer, topic « inbound events »)
  └─> EventPersistenceMapper.apply()          persistance en tant que system user
        └─> getDeviceEventManagement().addDevice{Measurements,Alerts,...}(context, request)
              └─ getEventManagement() == KafkaEventPersistenceTriggers
                 (décorateur câblé dans EventManagementTenantEngine.tenantInitialize :
                  `new KafkaEventPersistenceTriggers(this, implementation)`)
                 ├─> super.addDeviceXxx() ──> InfluxDbDeviceEventManagement ──> écriture InfluxDB
                 └─> forwardEvents() ──> OutboundPayloadEnrichmentLogic.enrichAndDeliver()
                       └─> getOutboundEventsProducer().send() ──> topic « outbound events »
```

Fichiers correspondants (liens relatifs à la racine du dépôt) :

- [`EventPersistencePipeline.java`](service-event-management/src/main/java/com/sitewhere/event/kafka/EventPersistencePipeline.java) — consommateur Kafka du topic inbound events.
- [`EventPersistenceMapper.java`](service-event-management/src/main/java/com/sitewhere/event/kafka/EventPersistenceMapper.java) — persistance via l'API event management en tant que system user.
- [`EventManagementTenantEngine.java`](service-event-management/src/main/java/com/sitewhere/event/microservice/EventManagementTenantEngine.java) — câblage du décorateur dans `tenantInitialize()` : `new KafkaEventPersistenceTriggers(this, implementation)`.
- [`KafkaEventPersistenceTriggers.java`](service-event-management/src/main/java/com/sitewhere/event/kafka/KafkaEventPersistenceTriggers.java) — décorateur : persistance (`super.addDeviceXxx()`) puis `forwardEvents()`.
- [`InfluxDbDeviceEventManagement.java`](service-event-management/src/main/java/com/sitewhere/event/persistence/influxdb/InfluxDbDeviceEventManagement.java) — écriture effective dans InfluxDB.
- [`OutboundPayloadEnrichmentLogic.java`](service-event-management/src/main/java/com/sitewhere/event/processing/OutboundPayloadEnrichmentLogic.java) — enrichissement du payload puis remise au producer.
- [`OutboundEventsProducer.java`](service-event-management/src/main/java/com/sitewhere/event/kafka/OutboundEventsProducer.java) — envoi vers le topic outbound events.

Deux nuances sur la « garantie » :

- **Ordre garanti, pas de livraison garantie.** L'écriture InfluxDB s'exécute
  strictement avant l'envoi outbound, dans le même appel du décorateur : si
  l'écriture InfluxDB échoue, `forwardEvents()` n'est jamais atteint (pas
  d'envoi outbound sans persistance). Mais `EventPersistenceMapper.apply()`
  attrape l'exception, loggue « Unable to persist device event » et retourne
  `null` : l'événement est perdu, sans retry (le comportement de commit
  d'offsets de `DirectKafkaConsumer` n'est pas vérifiable dans ce dépôt, la
  classe vit dans la dépendance `sitewhere-microservice`).
- **Pas d'atomicité entre les deux envois.** Le `send()` vers outbound est un
  producer Kafka asynchrone, sans transaction ni lien avec l'écriture Influx :
  un crash entre les deux produirait exactement le désync (InfluxDB écrit,
  outbound jamais envoyé) que cette branche corrige.

## Objectif

- N'avoir qu'**un seul envoi vers InfluxDB** : le service ne fait que la
  persistance.
- Un composant à part expose l'endpoint HTTP `/write` nécessaire à une
  **subscription InfluxDB** sur cette même base : InfluxDB recopie chaque
  écriture acceptée vers ce endpoint.
- Quand l'event revient par cet endpoint, il est reconverti, puis envoyé sur
  **Kafka outbound**.

C'est donc InfluxDB qui déclenche l'envoi outbound, via sa subscription, au lieu
du service lui-même :

```
Kafka inbound ──> event-management ──> écriture dans InfluxDB (seule écriture)
                                          │
                                          └─ (thread HTTP dédié) subscription InfluxDB
                                               recopie les lignes vers /write du service
                                               └─> reconversion en event
                                                    └─> Kafka outbound
```

Le push direct vers outbound est désactivé quand le datastore est InfluxDB : la
route normale (forwarding direct Kafka, utilisée pour MongoDB) n'est plus active.

## Détail des modifications

### `build.gradle`

- Bump de la dépendance `sitewhere.microservice.api.version` : `3.0.11f` → `3.0.11g`.

### Nouveau package `com.sitewhere.event.influxwrite`

| Classe | Rôle |
|---|---|
| `InfluxLine` | Modèle d'une ligne du line protocol (measurement, tags, fields, timestamp en nanosecondes). |
| `InfluxLineProtocol` | Parseur du payload line protocol v1 (échappements, sections tags/fields gérés par un `Scanner` interne). |
| `InfluxWriteEventConverter` | Convertit une `InfluxLine` en `IDeviceEvent` et reconstruit le `IDeviceEventContext` (résolution device/assignment via le tenant engine). |
| `InfluxWriteEndpoint` | Endpoint HTTP (`IMetricsHttpEndpoint`) servi sur le serveur de métriques (thread HTTP séparé du pipeline de persistance), chemin `/write`, paramètre `db` pour identifier la base InfluxDB. Compteurs Prometheus (`sitewhere_influxwrite_*`). |

### `service-event-management/.../kafka/KafkaEventPersistenceTriggers.java`

- Nouveau flag `forwardToKafka` (défaut `true`), désactivé automatiquement au
  démarrage quand le datastore du tenant est de type `influxdb`
  (`TimeSeriesProvider.INFLUX_DB`).
- Dans ce cas, `forwardEvents()` ne pousse plus au topic outbound : c'est la
  subscription InfluxDB qui signale chaque écriture au endpoint `/write`, et
  c'est ce retour qui déclenche l'envoi outbound. Un log signale ce mode.
- En cas d'échec de résolution du datastore, on retombe sur le comportement
  historique (forwarding direct Kafka) avec un warning.

### `service-event-management/.../microservice/EventManagementTenantEngine.java`

- Nouvelle méthode `registerInfluxDatabaseIfApplicable()` : si le datastore du
  tenant est InfluxDB, le nom de base est récupéré depuis le `InfluxDbClient`
  injecté et le tenant engine s'enregistre auprès du microservice comme
  propriétaire de cette base. Appelée dans `tenantInitialize()`.
- `tenantStop()` appelle `unregisterInfluxDatabase()` pour arrêter le forwarding
  des écritures de la base du tenant.
- Nouveaux champ/accesseurs `influxDatabase` et helper
  `getEventManagementMicroservice()`.

### `service-event-management/.../microservice/EventManagementMicroservice.java`

- Instancie `InfluxWriteEndpoint` et l'enregistre sur le serveur de métriques
  (`getMetricsServer().registerEndpoint(...)`) au niveau microservice : l'endpoint
  vit dans le thread HTTP du serveur de métriques, indépendant du traitement des
  événements.
- Nouvelle map `tenantEnginesByInfluxDatabase` (`ConcurrentHashMap`) et
  implémentation des méthodes de `IEventManagementMicroservice` :
  `registerInfluxDatabase`, `unregisterInfluxDatabase`,
  `getTenantEngineForInfluxDatabase` (permet de router un write entrant vers le
  tenant propriétaire de la base).

### `service-event-management/.../spi/microservice/IEventManagementMicroservice.java`

- Ajout du contrat pour l'enregistrement/la résolution des bases InfluxDB :

```java
public void registerInfluxDatabase(String database, IEventManagementTenantEngine tenantEngine);
public void unregisterInfluxDatabase(String database);
public IEventManagementTenantEngine getTenantEngineForInfluxDatabase(String database);
```

## Tests (`src/test`)

- `InfluxLineProtocolTest` : parsing du line protocol (tags, fields, échappements, timestamps).
- `InfluxWriteEventConverterTest` : conversion ligne → événement et reconstruction du contexte.
- `InfluxWriteEndpointTest` : endpoint HTTP servi par un `HttpServer` JDK embarqué
  (405/400/404, résolution du paramètre `db`, 204 avec livraison au producer
  outbound, 500 si toutes les lignes échouent).

Lancement : `just test` (tous les services) ou `just test service-event-management`.

## Configuration InfluxDB attendue

Côté InfluxDB, il faut créer une subscription sur la base du tenant pointant vers
l'endpoint, par exemple :

```
CREATE SUBSCRIPTION "sitewhere_outbound" ON "<database>" DESTINATIONS ALL 'http://<host>:<metrics-port>/write?db=<database>'
```

Le paramètre `db` de la requête est ce qui permet au service de retrouver le tenant
engine propriétaire des lignes reçues.

## Dépendances externes de `service-event-management`

### Systèmes distants

| Système | Rôle | Détail |
|---|---|---|
| **Kafka** | Bus de communication central | Le pipeline `EventPersistencePipeline` consomme le topic des événements *preprocessed/decoupled* produits par *inbound-processing* ; `OutboundEventsProducer` et `OutboundCommandInvocationsProducer` publient vers les topics *outbound events* et *outbound command invocations*, consommés par *outbound-connectors*. |
| **Service *device-management*** | Résolution device/assignment | Appelé par gRPC (`IDeviceManagement`) pour l'enrichissement du contexte des événements (`InfluxWriteEventConverter.buildContext` et `OutboundPayloadEnrichmentLogic`). |
| **Clients gRPC externes** | API d'event management | `EventManagementGrpcServer` expose l'API `DeviceEventManagement` pour les autres microservices (event-sources, batch-operations, etc.). |
| **Datastore séries temporelles** | Persistance des événements | Un datastore par tenant via `DatastoreDefinition` / `TimeSeriesProvider` : InfluxDB pour cette branche ; Warp 10 et Cassandra également disponibles (`sitewhere-influxdb`, `sitewhere-warp10`, `sitewhere-cassandra`). MongoDB reste la route par défaut hors provider time-series. |
| **Serveur de métriques (HTTP)** | Scraping Prometheus + endpoint `/write` | Le `IMetricsHttpEndpoint` héberge désormais `/write` à côté de l'export Prometheus, sur le port metrics du microservice — c'est ce port que la subscription InfluxDB doit joindre. |
| **InfluxDB subscription** | Déclencheur outbound | Nouvelle dépendance configurationnelle : sans `CREATE SUBSCRIPTION ...` sur la base du tenant et à défaut de joignabilité réseau entre InfluxDB et le port metrics du service, aucun event n'est envoyé vers Kafka outbound. |

### Bibliothèques (versions pilotées par le `build.gradle` racine)

- API microservice : `com.sitewhere:sitewhere-grpc-client` **3.0.11g** (bump 3.0.11f → 3.0.11g lié à cette branche — fournit notamment les builders gRPC et le socle multitenant).
- Intégrations : `sitewhere-influxdb`, `sitewhere-warp10`, `sitewhere-cassandra` **3.0.2** (`sitewhere.integration.api.version`) — `sitewhere-influxdb` fournit le `InfluxDbClient` et les classes `InfluxDbDevice*` utilisées par le converter.
- Infrastructure : Kafka clients **2.0.0**, gRPC **1.25.0**, alignés sur le BOM Quarkus **1.7.2.Final** imposé au niveau racine.

La nouveauté de cette branche du point de vue des dépendances : aucun nouveau
driver ni bibliothèque externe, mais la dépendance **InfluxDB subscription**
et la joignabilité HTTP d'InfluxDB vers le port metrics du service deviennent
des prérequis fonctionnels du flux de persistance → outbound.

## Dépendances externes de `service-device-management`

`service-event-management` s'appuie sur `service-device-management` pour
l'enrichissement (device/assignment), il est donc utile de connaître ses propres
dépendances.

### Systèmes distants

| Système | Rôle | Détail |
|---|---|---|
| **Base relationnelle (RDB)** | Persistance du référentiel | `DeviceManagementTenantEngine` étend `RdbTenantEngine` : devices, assignments, assets, schedules sont stockés dans une base relationnelle (JDBC/Hibernate via `sitewhere-rdb`), définie par tenant via `DatastoreDefinition` (`getActiveConfiguration().getDatastore()`). Aucune persistance time-series ici, contrairement à event-management. |
| **Kafka** | Publication d'événements d'interaction | `DeviceInteractionEventsProducer` publie des *device interaction events* (ex. changements d'état d'assignment) vers le topic **inbound events** — c'est-à-dire l'entrée du pipeline qu'event-management consomme en aval. Device-management est donc aussi un producteur amont du flux inbound. |
| **Clients gRPC externes** | API de gestion des devices | `DeviceManagementGrpcServer` expose l'API `DeviceManagement` consommée par event-management (enrichissement du contexte via `IDeviceManagement`), event-sources, device-registration, etc. |
| **Serveur de métriques (HTTP)** | Scraping Prometheus | Même socle que les autres microservices (port metrics). |

### Bibliothèques

- API microservice : `com.sitewhere:sitewhere-grpc-client` **3.0.11g** (même version que event-management).
- Intégrations : `com.sitewhere:sitewhere-rdb` **3.0.2** (persistence relationnelle : `RdbTenantEngine`, entités JPA).
- Infrastructure : mêmes versions racine que event-management (Kafka clients 2.0.0, gRPC 1.25.0, BOM Quarkus 1.7.2.Final).

Chaîne de dépendance au complet dans le flux couvert par cette branche :

```
device-management ──(gRPC)──> event-management ──(Kafka inbound events)──> [pipeline]
        │                          │
        └─(Kafka inbound events)───┘   device-management est producteur amont
                                       (device interaction events)
event-management ──(gRPC DeviceManagement)──> device-management (enrichissement)
```

`service-device-management` n'est pas modifié par cette branche : sa seule
relation avec le flux InfluxDB est indirecte — il fournit les données
device/assignment nécessaires à la reconstruction du contexte
(`InfluxWriteEventConverter.buildContext`) au retour du `/write`.

## Création du schéma

Sur des infrastructures vierges (Kafka, InfluxDB, Postgres vides), le schéma est
provisionné automatiquement à l'initialisation des tenant engines :

- **Kafka** : chaque topic est créé par le premier composant qui s'y connecte
  (`KafkaTopicWaiter`, noyau `sitewhere-microservice`) avec
  `defaultTopicPartitions` / `defaultTopicReplicationFactor` tirés de la
  configuration d'infrastructure Kafka. Le nom des topics suit
  `{productId}.{namespace-k8s}.tenant.{tenant-name}.{suffix}` (`KafkaTopicNaming`).
- **InfluxDB** : la base du tenant est créée par *event-management* à
  l'initialisation du tenant engine (`InfluxDbClient.initialize()` →
  `CREATE DATABASE "%s"`). Le gabarit par défaut est `tenant_${tenant.id}`
  (retention `autogen`). InfluxDB n'a pas de DDL : le schéma de la measurement
  est créé implicitement par les points écrits par les classes `InfluxDbDevice*`.
- **Postgres (RDB)** : la base du tenant est créée par *device-management* au
  démarrage du tenant engine (`Postgres95Provider` : `tenant_%s`, exécuté après
  lecture de `pg_database`, avec `CREATE DATABASE` si absente), puis le schéma
  `devicemanagement` et ses tables sont créés par **Flyway**
  (`service-device-management/src/main/resources/db/migrations/tenants/devicemanagement/V1__schema_initialization.sql`,
  locations `db/migrations/tenants/%s` + `db/postgres/migrations/tenant`).
  Hibernate mappe ensuite les entités JPA du module sur ce schéma
  (`hibernate.default_schema`).

### Kafka — topics utilisés par les deux services

| Topic (suffixe) | Tenant / nom complet | Service imposant le schéma (base vierge) | Producteur / consommateur | Format des messages | Description |
|---|---|---|---|---|---|
| `inbound-events` | `sitewhere.{ns}.tenant.{tenant-name}.inbound-events` | Premier service connecté (partitions/RF depuis la config Kafka) ; format des messages imposé par *event-management* (consommateur) | Produit : *inbound-processing* (événements enrichis) et *device-management* (`DeviceInteractionEventsProducer`) ; consommé : *event-management* (`EventPersistencePipeline` → persistance) | Protobuf `GPreprocessedEventPayload` (artefact `sitewhere-grpc-event-management`), clé = UUID device | Entrée du pipeline de persistance des événements. C'est ce topic qui amène les événements traduits en écriture InfluxDB. |
| `outbound-events` | `sitewhere.{ns}.tenant.{tenant-name}.outbound-events` | *event-management* | Produit : *event-management* (`OutboundEventsProducer`, désormais alimenté par le retour du `/write`) ; consommé : *outbound-connectors* | Protobuf `GProcessedEventPayload` enrichi (`EventModelConverter`/`EventModelMarshaler`), clé = UUID device | Événements persistés puis enrichis ; sortie du flux couvert par cette branche. |
| `outbound-command-invocations` | `sitewhere.{ns}.tenant.{tenant-name}.outbound-command-invocations` | *event-management* | Produit : *event-management* (`OutboundCommandInvocationsProducer`) ; consommé : *command-delivery* | Protobuf `GProcessedEventPayload`, filtré sur les `CommandInvocation` | Sous-ensemble des commandes invoquées, pour la livraison de commandes. |

### InfluxDB — measurement écrite par *event-management*

| Measurement | Base de données | Service imposant le schéma (base vierge) | Description |
|---|---|---|---|
| `events` | `tenant_<tenant-id>` (créée par `InfluxDbClient` ; retention `autogen`) | *event-management* | Measurement unique pour tous les types d'événements. Tags : `type` (`Measurement`, `Location`, `Alert`, ...), `device`, `assignment`, `altid`, `area`, ... Champs par type : `mxname`/`mxvalue`/`rcvdate` (mesures), `latitude`/`longitude`/`elevation` (localisations), `message`/`level`/`alert` (alertes), etc. Définis par `InfluxDbDeviceEvent` + `InfluxDbDevice{Measurement,Location,Alert,CommandInvocation,CommandResponse,StateChange}` ; relus par `InfluxWriteEventConverter`. C'est exactement ce contenu que la subscription InfluxDB renvoie au `/write`. |

### Postgres (RDB) — tables créées par *device-management*

Base `tenant_<tenant-id>`, schéma `devicemanagement`, migration Flyway du module
`service-device-management` (V1__), couchée sur les entités JPA `Rdb*` du
package `persistence/rdb/entity`.

| Table (`devicemanagement.*`) | Description |
|---|---|
| `area` | Zones géographiques (hiérarchie via `parent_id`, token unique) |
| `area_boundary` | Points de la frontière d'une zone |
| `area_metadata` | Métadonnées clé/valeur d'une zone |
| `area_type` / `area_type_metadata` | Types de zones et leurs métadonnées |
| `contained_area_types` | Relation type de zone ↔ types de zones contenables |
| `customer` / `customer_metadata` | Clients (hiérarchie) et métadonnées |
| `customer_type` / `customer_type_metadata` | Types de clients et métadonnées |
| `contained_customer_types` | Relation type de client ↔ types contenables |
| `device` | Devices enregistrés ; source du `deviceId` et `token` du contexte |
| `device_metadata` | Métadonnées clé/valeur d'un device |
| `device_type` / `device_type_metadata` | Types de devices et métadonnées |
| `device_status` / `device_status_metadata` | Statuts possibles d'un type de device |
| `device_unit` / `device_unit_metadata` | Unités d'éléments d'un schéma de device |
| `device_slot` / `device_slot_metadata` | Slots d'unités d'un schéma de device |
| `device_element_schema` / `device_element_schema_metadata` | Schéma d'éléments (unités/slots) par type de device |
| `device_element_mapping` | Mapping élément ↔ slot pour un device donné |
| `device_assignment` | Affectations device → customer/area/asset ; source de l'`assignmentId` du contexte des événements |
| `device_assignment_metadata` | Métadonnées d'une affectation |
| `device_alarm` / `device_alarm_metadata` | Alarmes liées aux affectations |
| `device_command` / `device_command_metadata` | Commandes disponibles par type de device |
| `command_parameter` | Paramètres des commandes |
| `device_group` / `device_group_metadata` | Groupes de devices et métadonnées |
| `device_group_element` / `device_group_element_metadata` | Éléments d'un groupe et métadonnées |
| `device_group_roles` / `device_group_element_roles` | Rôles entre groupes et éléments |
| `location` | Dernier emplacement connu par affectation |
| `zone` / `zone_boundary` / `zone_metadata` | Zones dessinées associées aux affectations |

Ces tables n'existent pas pour *event-management* : ses données de référence
(device, assignment, customer, area) viennent de ce schéma via gRPC, et ses
événements vont dans la measurement InfluxDB ci-dessus.
