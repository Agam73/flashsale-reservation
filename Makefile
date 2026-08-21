include .env
export

MIGRATE := migrate -path migrations -database "$(DATABASE_URL)"

.PHONY: db-up db-down migrate-up migrate-down migrate-down-one migrate-create migrate-force psql smoke-test

## Start local Postgres via docker-compose.
db-up:
	docker compose up -d postgres

## Stop and remove local Postgres (data volume persists; use `docker compose down -v` to wipe it).
db-down:
	docker compose down

## Apply all pending migrations.
migrate-up:
	$(MIGRATE) up

## Roll back ALL migrations. Careful — this drops every table.
migrate-down:
	$(MIGRATE) down -all

## Roll back the single most recent migration.
migrate-down-one:
	$(MIGRATE) down 1

## Scaffold a new pair of migration files: make migrate-create name=add_something
migrate-create:
	migrate create -ext sql -dir migrations -seq $(name)

## Unstick a migration left in a dirty state after a failed run: make migrate-force version=3
migrate-force:
	$(MIGRATE) force $(version)

## Open a psql shell against the local database.
psql:
	psql "$(DATABASE_URL)"

## Run the schema smoke test (inserts/rolls back sample data to prove constraints hold).
smoke-test:
	psql "$(DATABASE_URL)" -v ON_ERROR_STOP=1 -f scripts/smoke_test.sql
