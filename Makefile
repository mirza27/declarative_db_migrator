# run container
run-base:
	docker compose -f docker-compose.base.yml --env-file .base.env up -d 

down-base:
	docker compose -f docker-compose.base.yml --env-file .base.env down

run-pub:
	docker compose -f docker-compose.publication.yml --env-file .publication.env up -d

down-pub:
	docker compose -f docker-compose.publication.yml --env-file .publication.env down

client:
	cd client && npm run dev

# RUN worker
executor:
	go run ./cmd/executor/main.go >  logs/executor.log 2>&1

parser:
	go run ./cmd/parser/main.go >  logs/parser.log 2>&1

checker:
	go run ./cmd/checker/main.go >  logs/checker.log 2>&1

joiner:
	go run ./cmd/joiner/main.go >  logs/joiner.log 2>&1

api:
	go run main.go


# Debezium Config
conn-publication:
	curl -X POST -H "Content-Type: application/json"   -d @internal/debezium/connector-publication.json   http://localhost:8083/connectors
dis-publication:
	curl -X DELETE http://localhost:8083/connectors/publication-connector

#kafka
reset-kafka:
	docker stop debezium
	docker exec -it kafka kafka-consumer-groups --bootstrap-server kafka:9092 --delete --group 1
	docker exec -it kafka kafka-topics --bootstrap-server kafka:9092 --delete --topic '.*'
	docker start debezium

# pivotable 
up-pivot:
	docker exec -i pivot_db psql -U pivot_user -d pivot < ./internal/config/pivot_db.sql

drop-pivot:
	docker exec -i pivot_db  psql -U pivot_user -d pivot < ./migration/pivot/down.sql

empty-pivot:
	docker exec -i pivot_db  psql -U pivot_user -d pivot < ./migration/pivot/empty.sql
	

# PUBLICATION CASE
add-new-publication:
	docker exec -i new_publication psql -U new_user -d new_publication_db < ./migration/publication/new/up.sql
	docker exec -i new_publication psql -U new_user -d new_publication_db < ./migration/publication/new/seed.sql

drop-new-publication:
	docker exec -i new_publication psql -U new_user -d new_publication_db < ./migration/publication/new/down.sql

empty-new-publication:
	docker exec -i new_publication psql -U new_user -d new_publication_db < ./migration/publication/new/empty.sql

add-old-publication:
	docker exec -i old_publication psql -U old_user -d old_publication_db < ./migration/publication/old/up.sql
	docker exec -i old_publication psql -U old_user -d old_publication_db < ./migration/publication/old/seed.sql

drop-old-publication:
	docker exec -i old_publication psql -U old_user -d old_publication_db < ./migration/publication/old/down.sql

empty-old-publication:
	docker exec -i old_publication psql -U old_user -d old_publication_db < ./migration/publication/old/empty.sql

pub-seed:
	docker exec -i old_publication psql -U old_user -d old_publication_db < ./migration/publication/old/empty.sql
	go run ./cmd/seeder/*.go



.PHONY: api client