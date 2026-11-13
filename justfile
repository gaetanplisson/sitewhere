export JAVA_TOOL_OPTIONS := "-Dfile.encoding=UTF-8"

core_dir := justfile_directory() / "../sitewhere-microservice-core"
# Fork of sitewhere-integration (datastores: sitewhere-influxdb, -rdb, -communication...), version sitewhere.integration.api.version
# of build.gradle, built against the sitewhere-microservice of core_dir (both published to the Maven local repository)
integration_dir := justfile_directory() / "../sitewhere-integration"
registry := "docker.io"
namespace := "gaetanplisson"

changed := "service-inbound-processing service-event-management service-device-registration service-command-delivery service-outbound-connectors service-device-management"

default:
    @just --list

publish-core:
    cd {{core_dir}} && ./gradlew publishToMavenLocal

publish-integration: publish-core
    cd {{integration_dir}} && ./gradlew publishToMavenLocal

build *services: publish-integration
    ./gradlew {{ if services == "" { "dockerImage" } else { prepend(":", append(":dockerImage", services)) } }} -PdockerRepository={{registry}} -PdockerNamespace={{namespace}}

build-changed: (build changed)

build-debug *services: publish-integration
    ./gradlew {{ if services == "" { "dockerImage" } else { prepend(":", append(":dockerImage", services)) } }} -PdockerRepository={{registry}} -PdockerNamespace={{namespace}} -Pdebug

push *services: publish-integration
    ./gradlew {{ if services == "" { "dockerPushImage" } else { prepend(":", append(":dockerPushImage", services)) } }} -PdockerRepository={{registry}} -PdockerNamespace={{namespace}}

push-changed: (push changed)

# Run tests for all services, or only for given services. e.g. `just test service-event-management`
test *services:
    ./gradlew {{ if services == "" { "test" } else { prepend(":", append(":test", services)) } }}

all: push
