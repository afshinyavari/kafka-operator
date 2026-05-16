1. always use base package name se.afshin.yavari
2. Always check the kind setup if networking and cluster components are working:
   - All kafka pods Running: `kubectl --context kind-kafka-{a,b,c} -n kafka get pods`
   - kube-proxy healthy: `kubectl --context kind-kafka-{a,b,c} -n kube-system get pods -l k8s-app=kube-proxy`
   - Flannel CNI healthy: `kubectl --context kind-kafka-{a,b,c} -n kube-flannel get pods`
   - Submariner gateway healthy: `kubectl --context kind-kafka-{a,b,c} -n submariner-operator get pods`
   - If any system pod is CrashLoopBackOff or Error, restart it and wait for Ready before proceeding
3. In the end always verify quorum and producer/consumers are working, always run in MCS mode (`make mcs-setup`):
   - Quorum: `make quorum`
   - Produce: `echo -e 'msg1\nmsg2\nmsg3' | timeout 15 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server <headless-svc>.kafka.svc.clusterset.local:9092 --topic T 2>/dev/null`
   - Consume: `timeout 15 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server <headless-svc>.kafka.svc.clusterset.local:9092 --topic T --from-beginning --max-messages 3 --timeout-ms 10000 2>/dev/null`
   - Verify offline (no network): `kafka-dump-log.sh --files /var/lib/kafka/data/<topic>-0/*.log --print-data-log`
4. Try to keep the reconcilers clean so break out business logic into other classes
5. Keep timeouts short when running the tests
