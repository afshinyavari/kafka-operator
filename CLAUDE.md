1. always use base package name se.afshin.yavari
2. When implementing a new feature and its time for testing
   - run (`make teardown`)
   - and run (`make mcs-setup`)
   - So we get a fresh setup for the first run
   - the teardown and setup can be done at the beginning in the background before you start coding, so we have a fresh setup for testing
3. In the end always verify quorum and producer/consumers are working, always run in MCS mode (`make mcs-setup`):
   - Quorum: `make quorum`
   - run end to end proxy test
   - Verify offline (no network): `kafka-dump-log.sh --files /var/lib/kafka/data/<topic>-0/*.log --print-data-log`
4. Try to keep the reconcilers clean so break out business logic into other classes
5. Keep timeouts short when running the tests
6. Write unit tests when implementing new features
7. Update documentation, architecture, crds, everything
9. Always ask before commit to git
