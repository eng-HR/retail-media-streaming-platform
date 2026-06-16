using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using RetailMedia.Api.Middleware;
using RetailMedia.Application.Interfaces;

namespace RetailMedia.Api.Tests;

public class TenantContextMiddlewareTests
{
    [Fact]
    public async Task InvokeAsync_WithXTenantIdHeader_SetsTenantContext()
    {
        var tc = new TenantContext();
        var body = await InvokeWithHeader(tc, "X-Tenant-Id", "tesco");

        Assert.Equal("tesco", tc.CurrentTenantId.ToString());
        Assert.Equal(HttpStatusCode.OK, body.StatusCode);
    }

    [Fact]
    public async Task InvokeAsync_WithoutTenantId_Returns401()
    {
        var tc = new TenantContext();
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddSingleton<ITenantContext>(tc);
        builder.WebHost.UseTestServer();

        var app = builder.Build();
        app.UseMiddleware<TenantContextMiddleware>();
        app.MapGet("/test", () => Results.Ok());

        await app.StartAsync();
        var client = app.GetTestClient();

        var response = await client.GetAsync("/test");

        Assert.Equal(HttpStatusCode.Unauthorized, response.StatusCode);
    }

    [Fact]
    public async Task InvokeAsync_HealthEndpoint_SkipsTenantCheck()
    {
        var tc = new TenantContext();
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddHealthChecks();
        builder.Services.AddSingleton<ITenantContext>(tc);
        builder.WebHost.UseTestServer();

        var app = builder.Build();
        app.UseMiddleware<TenantContextMiddleware>();
        app.MapHealthChecks("/healthz");

        await app.StartAsync();
        var client = app.GetTestClient();

        var response = await client.GetAsync("/healthz");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    static async Task<HttpResponseMessage> InvokeWithHeader(TenantContext tc, string header, string value)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddSingleton<ITenantContext>(tc);
        builder.WebHost.UseTestServer();

        var app = builder.Build();
        app.UseMiddleware<TenantContextMiddleware>();
        app.MapGet("/test", () => Results.Ok());

        await app.StartAsync();
        var client = app.GetTestClient();

        var request = new HttpRequestMessage(HttpMethod.Get, "/test");
        request.Headers.Add(header, value);
        return await client.SendAsync(request);
    }
}
