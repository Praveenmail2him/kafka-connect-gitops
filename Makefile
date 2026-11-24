# Deploy the orders service and the upstream Strimzi example onto OpenShift.
# The cluster token is never stored here. Log in first, or pass TOKEN to `make login`.

NAMESPACE ?= kafka-connect-gitops
SERVER ?= https://api.example.com:6443
KAFKA ?= demo-kafka
CONNECT ?= connect-tier1-cdc
APP ?= orders-service

.PHONY: help login check-login deploy namespace operator kafka topics app connect connectors network status smoke undeploy

# `make login <token>` passes the token as a second goal. Give that goal an
# empty recipe so make does not look for a file named after the token.
ifeq ($(filter login,$(MAKECMDGOALS)),login)
  LOGIN_TOKEN := $(firstword $(filter-out login,$(MAKECMDGOALS)))
  ifneq ($(LOGIN_TOKEN),)
    TOKEN ?= $(LOGIN_TOKEN)
    $(LOGIN_TOKEN):
	@:
  endif
endif

help:
	@echo "Targets:"
	@echo "  make login <oc-token>         Log in (token is not written to disk)"
	@echo "  make login TOKEN=<oc-token>   Same, with an explicit variable"
	@echo "  make deploy                   Install Strimzi, Kafka, Connect, and the orders service"
	@echo "  make status                   Show cluster resources"
	@echo "  make smoke                    POST an order and read it back"
	@echo "  make undeploy                 Delete the $(NAMESPACE) project"

login:
	@test -n "$(TOKEN)" || (echo "Usage: make login <oc-token> [SERVER=$(SERVER)]" && exit 1)
	@oc login --token="$(TOKEN)" --server="$(SERVER)" --insecure-skip-tls-verify

check-login:
	@oc whoami >/dev/null

deploy: check-login namespace operator kafka topics app connect connectors network status

namespace: check-login
	oc apply -f platform/base/namespace.yaml
	oc project $(NAMESPACE)

operator: namespace
	oc apply -f platform/base/operator.yaml
	@echo "Waiting for the Strimzi operator..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
	  phase=$$(oc get csv -n $(NAMESPACE) -o jsonpath='{range .items[*]}{.status.phase}{"\n"}{end}' 2>/dev/null | awk 'NF{print; exit}'); \
	  if [ "$$phase" = "Succeeded" ]; then \
	    echo "Operator is ready"; \
	    oc wait --for=condition=Established crd/kafkas.kafka.strimzi.io --timeout=180s; \
	    exit 0; \
	  fi; \
	  echo "  operator phase: $${phase:-pending}"; \
	  sleep 10; \
	done; \
	echo "Strimzi operator did not become ready"; \
	oc get subscription,installplan,csv,pods -n $(NAMESPACE); \
	exit 1

kafka: operator
	oc apply -f platform/base/kafka.yaml
	@echo "Waiting for Kafka $(KAFKA)..."
	oc wait kafka/$(KAFKA) -n $(NAMESPACE) --for=condition=Ready --timeout=900s

topics: kafka
	oc apply -f platform/base/topics.yaml
	oc wait kafkatopic/orders -n $(NAMESPACE) --for=condition=Ready --timeout=180s
	oc wait kafkatopic/license-lines -n $(NAMESPACE) --for=condition=Ready --timeout=180s
	oc wait kafkatopic/dlq.orders-file-sink -n $(NAMESPACE) --for=condition=Ready --timeout=180s

app: topics
	mvn -f orders-service/pom.xml -B test package
	oc get bc/$(APP) -n $(NAMESPACE) >/dev/null 2>&1 || oc new-build --name $(APP) --binary --strategy=docker --to=$(APP):latest -n $(NAMESPACE)
	oc start-build $(APP) --from-dir=orders-service --follow -n $(NAMESPACE)
	oc apply -f platform/base/orders-service.yaml
	image=$$(oc get istag $(APP):latest -n $(NAMESPACE) -o jsonpath='{.image.dockerImageReference}'); \
	oc set image deployment/$(APP) $(APP)="$$image" -n $(NAMESPACE)
	oc rollout status deployment/$(APP) -n $(NAMESPACE) --timeout=300s

connect: topics
	oc apply -f platform/base/metrics-configmap.yaml
	oc apply -f platform/base/connect.yaml
	@echo "Waiting for KafkaConnect $(CONNECT) (image build can take several minutes)..."
	oc wait kafkaconnect/$(CONNECT) -n $(NAMESPACE) --for=condition=Ready --timeout=1200s

connectors: connect
	oc apply -f connectors/tier1-cdc/license-file-source.yaml
	oc apply -f connectors/tier1-cdc/orders-file-sink.yaml
	oc wait kafkaconnector/license-file-source -n $(NAMESPACE) --for=condition=Ready --timeout=300s
	oc wait kafkaconnector/orders-file-sink -n $(NAMESPACE) --for=condition=Ready --timeout=300s

network: connectors
	oc apply -f platform/base/networkpolicy.yaml

status:
	oc get kafka,kafkanodepool,kafkatopic,kafkaconnect,kafkaconnector,deploy,route,pods -n $(NAMESPACE)

smoke:
	@host=$$(oc get route $(APP) -n $(NAMESPACE) -o jsonpath='{.spec.host}'); \
	echo "POST http://$$host/api/orders"; \
	curl -fsS -X POST "http://$$host/api/orders" \
	  -H 'content-type: application/json' \
	  -d '{"customer":"ada","sku":"SKU-1","quantity":2}'; \
	echo; \
	for i in 1 2 3 4 5 6; do \
	  body=$$(curl -fsS "http://$$host/api/orders"); \
	  echo "$$body" | grep -q SKU-1 && echo "$$body" && exit 0; \
	  sleep 5; \
	done; \
	echo "Order was not visible on GET /api/orders"; \
	exit 1

undeploy:
	oc delete project $(NAMESPACE) --wait=false
	@echo "Deletion of project $(NAMESPACE) started"
