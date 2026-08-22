.PHONY: up down logs maintenance-logs rebuild status test verify

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

maintenance-logs:
	docker compose logs -f --tail=100 maintenance

status:
	docker compose ps
	du -sh data/raw data/parquet data/spool data/static_gtfs 2>/dev/null || true
	df -h /
	docker compose exec -T collector python healthcheck.py
	docker compose exec -T maintenance python maintenance_healthcheck.py

rebuild:
	docker compose run --rm collector python rebuild_parquet.py

test:
	python -m unittest discover -s tests -v

verify:
	python -m compileall -q .
	python -m unittest discover -s tests -v
