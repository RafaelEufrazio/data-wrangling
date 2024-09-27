run-docker: docker-build docker-run

docker-build:
	docker build -t data-wrangling .

docker-run:
	docker run --rm --name data-wrangling -p 8000:8000 data-wrangling

run-local:
	uvicorn app.main:app --port 8000 --host 127.0.0.1 --reload