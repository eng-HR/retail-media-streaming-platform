using Moq;
using RetailMedia.Application.DTOs;
using RetailMedia.Application.Interfaces;
using RetailMedia.Application.Services;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Application.Tests;

public class InsightsServiceTests
{
    private readonly Mock<IRedisCache> _cacheMock;
    private readonly Mock<IMetricsRepository> _metricsRepoMock;
    private readonly IInsightsService _service;
    private static readonly TenantId TenantId = RetailMedia.Domain.ValueObjects.TenantId.From("tesco");
    private static readonly CampaignId CampaignId = RetailMedia.Domain.ValueObjects.CampaignId.From("cmp_789");

    public InsightsServiceTests()
    {
        _cacheMock = new Mock<IRedisCache>();
        _metricsRepoMock = new Mock<IMetricsRepository>();
        _service = new InsightsService(_cacheMock.Object, _metricsRepoMock.Object);
    }

    [Fact]
    public async Task GetClicksAsync_RedisHit_ReturnsRedisCountOnly()
    {
        _cacheMock.Setup(c => c.KeyExistsAsync(It.IsAny<string>())).ReturnsAsync(true);
        _cacheMock.Setup(c => c.GetCounterAsync(It.IsAny<string>())).ReturnsAsync(100);

        var result = await _service.GetClicksAsync(CampaignId, TenantId);

        Assert.Equal(100, result.Clicks);
        Assert.Equal("cmp_789", result.CampaignId);
        _metricsRepoMock.Verify(
            r => r.GetClickCountAsync(It.IsAny<CampaignId>(), It.IsAny<TenantId>(), It.IsAny<CancellationToken>()),
            Times.Never);
    }

    [Fact]
    public async Task GetClicksAsync_RedisMiss_FallsBackToDb()
    {
        _cacheMock.Setup(c => c.KeyExistsAsync(It.IsAny<string>())).ReturnsAsync(false);
        _metricsRepoMock.Setup(r => r.GetClickCountAsync(CampaignId, TenantId, default)).ReturnsAsync(50);

        var result = await _service.GetClicksAsync(CampaignId, TenantId);

        Assert.Equal(50, result.Clicks);
    }

    [Fact]
    public async Task GetImpressionsAsync_RedisHit_ReturnsRedisCountOnly()
    {
        _cacheMock.Setup(c => c.KeyExistsAsync(It.IsAny<string>())).ReturnsAsync(true);
        _cacheMock.Setup(c => c.GetCounterAsync(It.IsAny<string>())).ReturnsAsync(200);

        var result = await _service.GetImpressionsAsync(CampaignId, TenantId);

        Assert.Equal(200, result.Impressions);
    }

    [Fact]
    public async Task GetImpressionsAsync_RedisMiss_FallsBackToDb()
    {
        _cacheMock.Setup(c => c.KeyExistsAsync(It.IsAny<string>())).ReturnsAsync(false);
        _metricsRepoMock.Setup(r => r.GetImpressionCountAsync(CampaignId, TenantId, default)).ReturnsAsync(75);

        var result = await _service.GetImpressionsAsync(CampaignId, TenantId);

        Assert.Equal(75, result.Impressions);
    }

    [Fact]
    public async Task GetClickToBasketAsync_RedisHit_ReturnsRedisCountOnly()
    {
        _cacheMock.Setup(c => c.KeyExistsAsync(It.IsAny<string>())).ReturnsAsync(true);
        _cacheMock.Setup(c => c.GetCounterAsync(It.IsAny<string>())).ReturnsAsync(30);

        var result = await _service.GetClickToBasketAsync(CampaignId, TenantId);

        Assert.Equal(30, result.ClickToBasket);
    }

    [Fact]
    public async Task GetClickToBasketAsync_RedisMiss_FallsBackToDb()
    {
        _cacheMock.Setup(c => c.KeyExistsAsync(It.IsAny<string>())).ReturnsAsync(false);
        _metricsRepoMock.Setup(r => r.GetClickToBasketCountAsync(CampaignId, TenantId, default)).ReturnsAsync(10);

        var result = await _service.GetClickToBasketAsync(CampaignId, TenantId);

        Assert.Equal(10, result.ClickToBasket);
    }

    [Fact]
    public async Task GetMetricsAsync_WithMetricFilter_ReturnsFilteredResults()
    {
        var metrics = new List<CampaignMetric>
        {
            new(TenantId, CampaignId, MetricType.Clicks, 100, DateTime.UtcNow.Date),
            new(TenantId, CampaignId, MetricType.Impressions, 200, DateTime.UtcNow.Date),
        };

        _metricsRepoMock.Setup(r => r.GetMetricsAsync(CampaignId, TenantId, null, null, null, default))
            .ReturnsAsync(metrics);

        var result = await _service.GetMetricsAsync(CampaignId, TenantId);

        Assert.Equal(100, result.Clicks);
        Assert.Equal(200, result.Impressions);
        Assert.Null(result.ClickToBasket);
    }

    [Fact]
    public async Task GetMetricsAsync_WithDateRange_FiltersByDates()
    {
        var from = new DateTime(2026, 1, 1);
        var to = new DateTime(2026, 1, 31);

        _metricsRepoMock.Setup(r => r.GetMetricsAsync(CampaignId, TenantId, null, from, to, default))
            .ReturnsAsync(new List<CampaignMetric>());

        var result = await _service.GetMetricsAsync(CampaignId, TenantId, null, from, to);

        Assert.NotNull(result);
    }
}
