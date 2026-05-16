---
# One KRaft controller per cluster (part of the 3-node controller quorum)
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaNodePool
metadata:
  name: controllers-${CLUSTER_SUFFIX}
  namespace: kafka
  labels:
    kafka.yavari.afshin.se/cluster: my-kafka
spec:
  roles:
    - CONTROLLER
  replicas: 1
  storage:
    size: 1Gi
  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi
  config:
    log.retention.hours: "168"
---
# Broker pool — 1 broker per cluster (reduced for local testing; bump replicas for real workload)
apiVersion: kafka.yavari.afshin.se/v1alpha1
kind: KafkaNodePool
metadata:
  name: brokers-${CLUSTER_SUFFIX}
  namespace: kafka
  labels:
    kafka.yavari.afshin.se/cluster: my-kafka
spec:
  roles:
    - BROKER
  replicas: 1
  rackTopologyKey: "topology.kubernetes.io/zone"
  storage:
    size: 2Gi
  resources:
    requests:
      cpu: 200m
      memory: 512Mi
    limits:
      cpu: 1000m
      memory: 1Gi
  config:
    log.retention.hours: "168"
