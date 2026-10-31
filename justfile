export JAVA_TOOL_OPTIONS := "-Dfile.encoding=UTF-8"

core_dir := justfile_directory() / "../sitewhere-microservice-core"
registry := "docker.io"
namespace := "gaetanplisson"

changed := "service-inbound-processing service-event-management service-device-registration service-command-delivery service-outbound-connectors service-device-management"

default:
    @just --list

publish-core:
    cd {{core_dir}} && ./gradlew publishToMavenLocal

build *services: publish-core
    ./gradlew {{ if services == "" { "dockerImage" } else { prepend(":", append(":dockerImage", services)) } }} -PdockerRepository={{registry}} -PdockerNamespace={{namespace}}

build-changed: (build changed)

build-debug *services: publish-core
    ./gradlew {{ if services == "" { "dockerImage" } else { prepend(":", append(":dockerImage", services)) } }} -PdockerRepository={{registry}} -PdockerNamespace={{namespace}} -Pdebug

push *services: publish-core
    ./gradlew {{ if services == "" { "dockerPushImage" } else { prepend(":", append(":dockerPushImage", services)) } }} -PdockerRepository={{registry}} -PdockerNamespace={{namespace}}

push-changed: (push changed)

all: push
