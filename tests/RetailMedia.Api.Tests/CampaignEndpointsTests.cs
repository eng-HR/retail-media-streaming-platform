using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Moq;
using RetailMedia.Api.Endpoints;
using RetailMedia.Api.Middleware;
using RetailMedia.Application.DTOs;
using RetailMedia.Application.Interfaces;
using RetailMedia.Domain.ValueObjects;
using Xunit;

namespace RetailMedia.Api.Tests;

public class CampaignEndpointsTests
{
    private readonly Mock<IInsightsService> _insightsMock;

    public CampaignEndpointsTests()
    {
        _insightsMock = new Mock<IInsightsService>();
    }

    [Fact]
    public async Task GetClicks_ReturnsOk()
    {
        _insightsMock.Setup(s => s.GetClicksAsync(
                It.IsAny<CampaignId>(), It.IsAny<TenantId>(), default))
            .ReturnsAsync(new ClickResponse("cmp_789", 150, 1234567890));

        var result = await InvokeEndpoint("/ad/cmp_789/clicks");

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
    }

    [Fact]
    public async Task GetImpressions_ReturnsOk()
    {
        _insightsMock.Setup(s => s.GetImpressionsAsync(
                It.IsAny<CampaignId>(), It.IsAny<TenantId>(), default))
            .ReturnsAsync(new ImpressionResponse("cmp_789", 275, 1234567890));

        var result = await InvokeEndpoint("/ad/cmp_789/impressions");

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
    }

    [Fact]
    public async Task GetClickToBasket_ReturnsOk()
    {
        _insightsMock.Setup(s => s.GetClickToBasketAsync(
                It.IsAny<CampaignId>(), It.IsAny<TenantId>(), default))
            .ReturnsAsync(new ClickToBasketResponse("cmp_789", 40, 1234567890));

        var result = await InvokeEndpoint("/ad/cmp_789/clickToBasket");

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
    }

    [Fact]
    public async Task GetMetrics_ReturnsOk()
    {
        _insightsMock.Setup(s => s.GetMetricsAsync(
                It.IsAny<CampaignId>(), It.IsAny<TenantId>(),
                It.IsAny<string?>(), It.IsAny<DateTime?>(),
                It.IsAny<DateTime?>(), default))
            .ReturnsAsync(new MetricsResponse("cmp_789", 100, 200, 30, null, null));

        var result = await InvokeEndpoint("/ad/cmp_789/metrics");

        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
    }

    [Fact]
    public async Task HealthCheck_ReturnsOk()
    {
        var result = await InvokeEndpoint("/healthz");
        Assert.Equal(HttpStatusCode.OK, result.StatusCode);
    }

    async Task<HttpResponseMessage> InvokeEndpoint(string path)
    {
        var builder = WebApplication.CreateBuilder();
        builder.Services.AddRouting();
        builder.Services.AddHealthChecks();
        builder.Services.AddScoped<ITenantContext>(_ =>
        {
            var tc = new TenantContext();
            tc.SetTenantId(TenantId.From("tesco"));
            return tc;
        });
        builder.Services.AddSingleton(_insightsMock.Object);
        builder.WebHost.UseTestServer();

        var app = builder.Build();
        app.UseMiddleware<ErrorHandlingMiddleware>();
        app.MapCampaignEndpoints();
        app.MapHealthChecks("/healthz");

        await app.StartAsync();

        var client = app.GetTestClient();
        var request = new HttpRequestMessage(HttpMethod.Get, path);
        if (!path.StartsWith("/health"))
            request.Headers.Add("X-Tenant-Id", "tesco");

        return await client.SendAsync(request);
    }
}
