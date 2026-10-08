update-deps:
	bash ./scripts/update-deps.sh

# Model checks the TLA+ specs (tla/README.md), needs Java. Without arguments
# checks the default set of every spec. SPEC=client checks one spec, with
# CONFIGS=all every config of it, or CONFIGS="MC_small bug_no_join_gate".
tla:
	sh tla/check.sh $(SPEC) $(CONFIGS)

.PHONY: update-deps tla
