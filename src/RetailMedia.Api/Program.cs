using Microsoft.EntityFrameworkCore;
using RetailMedia.Api.Endpoints;
using RetailMedia.Api.Middleware;
using RetailMedia.Application;
using RetailMedia.Application.Interfaces;
using RetailMedia.Infrastructure;
using RetailMedia.Infrastructure.Persistence;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddApplication();
builder.Services.AddInfrastructure(builder.Configuration);
builder.Services.AddScoped<ITenantContext, TenantContext>();
builder.Services.AddHealthChecks();
builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();

// Auto-apply EF Core migrations
using (var scope = app.Services.CreateScope())
{
    var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
    db.Database.Migrate();
}

app.UseMiddleware<ErrorHandlingMiddleware>();
app.UseMiddleware<TenantContextMiddleware>();

app.MapCampaignEndpoints();
app.MapHealthChecks("/healthz");

app.Run();

public partial class Program { }
