up-d:
	docker compose up -d

build-up-d:
	docker compose up --build -d

up:
	docker compose up

build-up:
	docker compose up --build

consumer-logs:
	docker logs --tail 10 consumer

producer-logs:
	docker logs --tail 10 producer

down:
	docker compose down