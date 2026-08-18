.PHONY: up down logs rebuild status

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

status:
	docker compose ps
	du -sh data/raw data/parquet 2>/dev/null || true
	df -h /

rebuild:
	docker compose run --rm collector python rebuild_parquet.py
