DOTNET := dotnet

.PHONY: build test run clean dev docker-up docker-down

build:
	$(DOTNET) build

test:
	$(DOTNET) test

run-api:
	$(DOTNET) run --project src/RetailMedia.Api

run-processor:
	$(DOTNET) run --project src/RetailMedia.StreamProcessor

run-collector:
	$(DOTNET) run --project src/RetailMedia.EventCollector

dev:
	docker-compose up -d postgres redis kafka
	@echo "Waiting for dependencies..."
	@sleep 5
	@echo "Starting services..."
	$(DOTNET) run --project src/RetailMedia.Api &
	$(DOTNET) run --project src/RetailMedia.StreamProcessor &
	$(DOTNET) run --project src/RetailMedia.EventCollector &

docker-up:
	docker-compose up --build -d

docker-down:
	docker-compose down

clean:
	$(DOTNET) clean
	rm -rf **/bin **/obj

restore:
	$(DOTNET) restore

lint:
	$(DOTNET) format --verify-no-changes

coverage:
	$(DOTNET) test --collect:"XPlat Code Coverage" --results-directory:TestResults

coverage-report: coverage
	dotnet tool install -g dotnet-reportgenerator-globaltool 2>/dev/null || true
	reportgenerator "-reports:TestResults/**/coverage.cobertura.xml" "-targetdir:coveragereport" -reporttypes:Html

.PHONY: build test run-api run-processor run-collector dev docker-up docker-down clean restore lint coverage coverage-report
