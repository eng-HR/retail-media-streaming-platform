using System.Net;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using RetailMedia.Api.Middleware;

namespace RetailMedia.Api.Tests;

public class ErrorHandlingMiddlewareTests
{
    [Fact]
    public async Task ArgumentException_Returns400()
    {
        var (status, body) = await InvokeWithError(() =>
            throw new ArgumentException("Invalid input"));

        Assert.Equal(HttpStatusCode.BadRequest, status);
        Assert.Contains("Invalid input", body);
    }

    [Fact]
    public async Task InvalidOperationException_Returns500()
    {
        var (status, body) = await InvokeWithError(() =>
            throw new InvalidOperationException("Something broke"));

        Assert.Equal(HttpStatusCode.InternalServerError, status);
        Assert.Contains("An unexpected error occurred", body);
    }

    [Fact]
    public async Task Exception_Returns500()
    {
        var (status, body) = await InvokeWithError(() =>
            throw new Exception("Unexpected error"));

        Assert.Equal(HttpStatusCode.InternalServerError, status);
    }

    [Fact]
    public async Task NoException_ReturnsOk()
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.WebHost.UseTestServer();

        var app = builder.Build();
        app.UseMiddleware<ErrorHandlingMiddleware>();
        app.MapGet("/ok", () => Results.Ok(new { message = "success" }));

        await app.StartAsync();
        var client = app.GetTestClient();

        var response = await client.GetAsync("/ok");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task ArgumentNullException_Returns400()
    {
        var (status, body) = await InvokeWithError(() =>
            throw new ArgumentNullException("param1"));

        Assert.Equal(HttpStatusCode.BadRequest, status);
    }

    static async Task<(HttpStatusCode status, string body)> InvokeWithError(Action action)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.WebHost.UseTestServer();

        var app = builder.Build();
        app.UseMiddleware<ErrorHandlingMiddleware>();
        app.MapGet("/error", () =>
        {
            action();
            return Results.Ok();
        });

        await app.StartAsync();
        var client = app.GetTestClient();

        var response = await client.GetAsync("/error");
        var body = await response.Content.ReadAsStringAsync();

        return (response.StatusCode, body);
    }
}
