PYTHON ?= python
EVENTS ?= 1000
.PHONY: build test test-race test-integration lint up down migrate setup-cdc verify-cdc seed generate-events process-events dbt-build dbt-test reconcile benchmark failure-test clean
build test test-race lint up down migrate setup-cdc verify-cdc seed process-events dbt-build dbt-test reconcile failure-test clean:
	$(PYTHON) scripts/manage.py $@
generate-events benchmark:
	$(PYTHON) scripts/manage.py $@ --events $(EVENTS)
test-integration:
	$(PYTHON) scripts/manage.py test-integration --events 10000
