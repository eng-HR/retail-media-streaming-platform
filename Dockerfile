# Build stage
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY . .
RUN dotnet restore src/RetailMedia.Api/RetailMedia.Api.csproj
RUN dotnet publish src/RetailMedia.Api/RetailMedia.Api.csproj -c Release -o /app/api

RUN dotnet restore src/RetailMedia.StreamProcessor/RetailMedia.StreamProcessor.csproj
RUN dotnet publish src/RetailMedia.StreamProcessor/RetailMedia.StreamProcessor.csproj -c Release -o /app/processor

RUN dotnet restore src/RetailMedia.EventCollector/RetailMedia.EventCollector.csproj
RUN dotnet publish src/RetailMedia.EventCollector/RetailMedia.EventCollector.csproj -c Release -o /app/collector

# Runtime stage for API
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS api
WORKDIR /app
COPY --from=build /app/api .
EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "RetailMedia.Api.dll"]

# Runtime stage for Stream Processor
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS processor
WORKDIR /app
COPY --from=build /app/processor .
ENTRYPOINT ["dotnet", "RetailMedia.StreamProcessor.dll"]

# Runtime stage for Event Collector
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS collector
WORKDIR /app
COPY --from=build /app/collector .
EXPOSE 8080
ENV ASPNETCORE_URLS=http://+:8080
ENTRYPOINT ["dotnet", "RetailMedia.EventCollector.dll"]
