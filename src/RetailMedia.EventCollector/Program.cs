using RetailMedia.Application;
using RetailMedia.EventCollector.Endpoints;
using RetailMedia.Infrastructure;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddApplication();
builder.Services.AddInfrastructure(builder.Configuration);

var app = builder.Build();

app.MapEventEndpoints();
app.MapHealthChecks("/healthz");

app.Run();
