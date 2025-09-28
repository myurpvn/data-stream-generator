generator-up:
	docker compose up -d

generator-down:
	docker compose down

l=10
consumer-logs:
	docker logs --tail $(l) consumer

producer-logs:
	docker logs --tail $(l) producer