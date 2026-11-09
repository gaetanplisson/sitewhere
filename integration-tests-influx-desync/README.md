# Integration tests — InfluxDB desync (`desync-influx`)

Tests d'intégration du correctif de désynchronisation InfluxDB décrit dans
`../influx-desync-docs.md`, exécutés sur un cluster **Kind**. Les microservices
SiteWhere attendent un plan de contrôle Kubernetes (CRDs
`SiteWhereInstance`/`SiteWhereTenant`) pour s'initialiser — leur configuration
(datastores, Kafka, Redis) est livrée par CRDs — c'est pourquoi le stack
docker-compose initial a été remplacé par une délégation au `deploy-kind` du
submodule `deploiments/`.

## Utilisation

```bash
cd integration-tests-influx-desync
just create-kind    # cluster Kind à 2 nœuds (1 control-plane + 1 worker)
just deploy-kind
```

`create-kind` crée le cluster avec la config locale `kind-config.yaml`
(`kindest/node:v1.24.17`, mappings de ports du control-plane ; deux nœuds
seulement au lieu des 6 workers de `deploiments/techno/kind`). Toutes les
variables sont surchargeables par l'environnement : `CLUSTER_NAME` (défaut
`influx-desync`, d'où le contexte `kind-influx-desync` utilisé par la suite),
`KUBE_CONTEXT`, `INSTANCE_NAME`, `INSTANCE_CONFIG`, `INSTANCE_REPO`.

Le Justfile appelle `deploiments/techno/sitewhere/Justfile deploy-kind`, qui :

1. déploie l'infrastructure SiteWhere (namespace `sitewhere-system` avec
   injection Istio, secret du registre INRIA, chart Helm : Strimzi/Kafka,
   PostgreSQL, InfluxDB, Redis, Mosquitto, Keycloak, opérateur SiteWhere) ;
2. installe les WasmPlugins echarde en mode **passthrough** (pas de route vers
   un cluster distant : chaque requête va à la base locale) ;
3. déploie sans Colvert ;
4. crée l'instance `INSTANCE_NAME` (`swctl`, config `test`, images
   `INSTANCE_REPO`, `imagePullPolicy Always`) — après avoir attendu que
   l'endpoint HTTP de Keycloak réponde (sinon le bootstrap de l'instance
   échoue de façon permanente : `BootstrapFailed` est mémorisé dans le
   status de la `SiteWhereInstance` et jamais retenté), puis
   **scale à 0 tous les microservices sauf device-management,
   event-management, instance-management, inbound-processing, event-sources
   et asset-management** (ce dernier est requis par le dataset `construction`
   du tenant : device-management attend son dataset avant de créer les
   devices ; event-sources et inbound-processing gardent testable le
   pipeline complet — messages JSON des devices venus du MQTT → topic
   `inbound-events`). `event-search` et `streaming-media`, absents de la
   config `test`, ne figurent pas dans la liste des microservices désactivés.

Variables utiles :

| Variable | Défaut | Rôle |
|---|---|---|
| `KUBE_CONTEXT` | (contexte courant) | cluster Kind ciblé |
| `INSTANCE_NAME` | `my-sitewhere` | nom de l'instance créée par swctl |
| `INSTANCE_CONFIG` | `test` | template de configuration de l'instance |
| `INSTANCE_REPO` | `gaetanplisson` | repo des images des microservices |

Autres commandes : `just delete-kind` (suppression du cluster),
`just status` (état du déploiement), `just restart-microservices [services...]`
(redémarrage un par un, en attendant la fin du démarrage de chacun ; event-management
y tourne sur l'image de debug, retirée à chaque fois), `just publish-debug-image`
(build de l'image de debug d'event-management depuis cette branche et push du tag
`debug-async`), `just reset` (suppression de l'instance et
des bases `tenant_*`, puis redéploiement par `deploy-kind` — l'infrastructure
est conservée), `just undeploy` (suppression instance + interfaces +
infrastructure).

## Scénario de test du correctif

Avec ce déploiement, la chaîne du correctif est testable de bout en bout :

1. les deux microservices tournent, event-management utilise InfluxDB comme
   datastore time-series (référence `influxdb` de la config d'instance) ;
2. un event persisté par event-management est écrit **une seule fois** dans
   InfluxDB et n'est pas poussé directement au topic outbound
   (`KafkaEventPersistenceTriggers.forwardToKafka = false`) ;
3. la **subscription InfluxDB** doit être créée sur la base du tenant
   (`tenant_<tenant-id>`) pour renvoyer les lignes au `/write` du serveur de
   métriques d'event-management — sans elle, rien n'arrive sur
   `outbound-events` :

```bash
# Depuis une console du pod InfluxDB (hostname: sitewhere-system-influxdb)
influx -execute "CREATE SUBSCRIPTION sitewhere_outbound ON \"tenant_<tenant-id>\".\"autogen\" DESTINATIONS ALL 'http://event-management.<instance>:9090/write?db=tenant_<tenant-id>'"
```

   `just check-influx-subscriptions [tenant]` vérifie qu'elle existe et qu'elle
   vise bien event-management (son service, ou l'IP de son pod courant) ;

4. le retour au `/write` reconstruit l'événement (`InfluxLineProtocol`,
   `InfluxWriteEventConverter`) puis livre le payload enrichi au topic
   `outbound-events` (vérifiable avec kafka-ui, interfaces déployées par
   `deploy`) ;
5. les compteurs Prometheus `sitewhere_influxwrite_*` exposés sur le port
   metrics (`/metrics`) suivent lignes reçues, événements livrés et échecs.

## Test de bout en bout

`just test-desync-e2e [tenant]` vérifie le correctif sur le cluster, avec des
messages au nom unique (conservés dans InfluxDB et Kafka) :

1. avec la subscription, l'event est écrit une seule fois dans InfluxDB, livré par
   `/write` (compteurs `sitewhere_influxwrite_*`) et présent une seule fois sur
   `outbound-events` ;
2. sans la subscription, retirée le temps de ce test puis toujours restaurée,
   l'event est dans InfluxDB mais pas sur `outbound-events` : c'est bien la
   subscription, et non le chemin de persistance, qui alimente le topic.

## Injection d'événements

Avec event-sources et inbound-processing actifs, la chaîne complète est
testable via `just send-iot-message` — la voie MQTT des devices de
`deploiments/iot/iot` (echo-clock) : DeviceMeasurement JSON publié sur
`SiteWhere/<tenant>/input/json` (NodePort 30883 de la gateway Istio) sur le
device disponible (première affectation active résolue via l'API REST), puis
relu depuis l'API ; `just check-iot-message-influx` envoie le même message puis
vérifie que le point est bien arrivé dans la base InfluxDB du tenant.
Instance-management doit aussi tourner :
`kubectl patch swm -n <instance> event-sources --type merge -p '{"spec":{"replicas":1}}'`
(idem `inbound-processing`, `instance-management`). L'injection directe reste
possible :

- sur le topic Kafka **inbound** du tenant (payload protobuf
  `GPreprocessedEventPayload`, consommé par `EventPersistencePipeline`), ou
- via l'API gRPC `DeviceEventManagement` d'event-management, les métadonnées
  device/assignment venant de device-management (base RDB PostgreSQL).

## Topics Kafka sur cluster vierge

Sur un cluster tout juste créé, un topic consommé mais jamais produit doit
être créé à la main, sinon `DecodedEventsPipeline` (inbound-processing) reste
bloqué en erreur `UnknownTopicOrPartitionException` sans rien consommer —
il réclame les partitions de tous ses topics d'un seul appel
(`KafkaUtils.getPartitions`) :

- `inbound-reprocess-events` : entrée de reprise administrative, aucun
  producteur dans le code ; à créer une fois par cluster et par tenant :

```bash
kubectl --context kind-influx-desync -n sitewhere-system apply -f - <<'EOF'
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaTopic
metadata:
  name: my-sitewhere.my-sitewhere.tenant.default.inbound-reprocess-events
  labels: {strimzi.io/cluster: sitewhere-kafka}
spec:
  topicName: my-sitewhere.my-sitewhere.tenant.default.inbound-reprocess-events
  partitions: 8
  replicas: 1
EOF
```

Les topics produits (`event-source-decoded-events`, `inbound-events`,
`outbound-events`, ...) sont auto-créés par leur producteur. La CR
`KafkaTopic` survit aux `reset` (l'instance est recréée, pas le cluster) :
elle n'est à recréer qu'après `delete-kind` ou `undeploy`.
