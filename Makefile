.PHONY: pg-schemas

SCHEMA_TEMPLATE = internal/postgres_sql/schema.sql
SCHEMA_JSONB    = internal/postgres_sql/schema_jsonb.sql
SCHEMA_BINARY   = internal/postgres_sql/schema_binary.sql

pg-schemas: $(SCHEMA_JSONB) $(SCHEMA_BINARY)
	@echo "Generated $(SCHEMA_JSONB) and $(SCHEMA_BINARY)"

$(SCHEMA_JSONB): $(SCHEMA_TEMPLATE)
	@echo '-- Auto-generated from schema.sql — do not edit.' > $@
	@sed -n '/^-- Stream Table/,$$p' $< | sed -e 's/__DATA_TYPE__/JSONB/g' -e 's/__PREFIX__/cf_map_/g' >> $@

$(SCHEMA_BINARY): $(SCHEMA_TEMPLATE)
	@echo '-- Auto-generated from schema.sql — do not edit.' > $@
	@sed -n '/^-- Stream Table/,$$p' $< | sed -e 's/__DATA_TYPE__/BYTEA/g' -e 's/__PREFIX__/cf_binary_map_/g' >> $@
