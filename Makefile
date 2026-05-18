SHELL := /bin/bash

.PHONY: setup mcs-setup teardown status quorum build image reload-image \
        kroxy-image reload-kroxy-image apicurio-proxy-image reload-apicurio-proxy-image \
        logs-a logs-b logs-c pods-a pods-b pods-c cr-status help

setup mcs-setup teardown status quorum build image reload-image \
kroxy-image reload-kroxy-image apicurio-proxy-image reload-apicurio-proxy-image \
logs-a logs-b logs-c pods-a pods-b pods-c cr-status help:
	$(MAKE) -C kind $@
