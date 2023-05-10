all: 
	docker-compose up --build

stop:
	docker-compose stop

resume:
	docker-compose start

clean:
	docker-compose down
	docker system prune -af
	docker volume prune -f

re: clean all