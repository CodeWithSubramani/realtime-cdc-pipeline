.PHONY: install test lint up register generate check down

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	ruff check .

up:
	docker compose up -d --build

register:
	curl -sf -X POST -H "Content-Type: application/json" \
	  --data @connectors/shop-postgres-source.json http://localhost:8083/connectors | python -m json.tool

generate:
	python scripts/generate_traffic.py --seconds 60

check:   ## compare source vs replica row counts (live rows only)
	@echo "source:    $$(psql postgresql://shop:shop@localhost:5432/shop -Atc 'select count(*) from orders')"
	@echo "warehouse: $$(psql postgresql://warehouse:warehouse@localhost:5433/warehouse -Atc 'select count(*) from orders_current where not _deleted')"

down:
	docker compose down -v
