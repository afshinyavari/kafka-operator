SHELL := /bin/bash

.PHONY: setup mcs-setup teardown status quorum proxy-test rbac-test \
        apicurio-rbac-test xml-filter-test \
        build image reload-image \
        kroxy-image reload-kroxy-image apicurio-proxy-image reload-apicurio-proxy-image \
        connect-image reload-connect-image kafka-connect-smoke-test \
        logs-a logs-b logs-c pods-a pods-b pods-c cr-status help \
        e2e e2e-extended e2e-full

setup mcs-setup teardown status quorum proxy-test rbac-test \
apicurio-rbac-test xml-filter-test \
build image reload-image \
kroxy-image reload-kroxy-image apicurio-proxy-image reload-apicurio-proxy-image \
connect-image reload-connect-image kafka-connect-smoke-test \
logs-a logs-b logs-c pods-a pods-b pods-c cr-status help \
e2e e2e-extended e2e-full:
	$(MAKE) -C kind $@
