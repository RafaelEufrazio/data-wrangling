run-docker-dev: docker-build docker-run-reload
	
run-docker: docker-build docker-run

docker-run-reload:
	docker run --rm --name data-wrangling -p 8000\:8000 -v ./app\:/code/app data-wrangling --reload

docker-run:
	docker run --rm --name data-wrangling -p 8000:8000 data-wrangling

docker-build:
	docker build -t data-wrangling .
	
	
run-local: local-install local-run

local-run:
	uvicorn app.main:app --port 8000 --host 127.0.0.1 --reload

local-install:
	pip install -r requirements.txt