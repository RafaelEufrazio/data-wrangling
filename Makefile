docker-build:
	docker build -t data-wrangling .

docker-run:
	docker run -p 8000:8000 data-wrangling