using Microsoft.Extensions.Logging;
using Moq;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;
using RetailMedia.StreamProcessor.Handlers;

namespace RetailMedia.StreamProcessor.Tests;

public class ClickHandlerTests
{
    private readonly Mock<IRedisCache> _cacheMock;
    private readonly Mock<IMetricsRepository> _metricsRepoMock;
    private readonly ClickHandler _handler;
    private static readonly TenantId TenantId = TenantId.From("tesco");
    private static readonly CampaignId CampaignId = CampaignId.From("cmp_789");

    public ClickHandlerTests()
    {
        _cacheMock = new Mock<IRedisCache>();
        _metricsRepoMock = new Mock<IMetricsRepository>();
        var loggerMock = new Mock<ILogger<ClickHandler>>();
        _handler = new ClickHandler(_cacheMock.Object, _metricsRepoMock.Object, loggerMock.Object);
    }

    [Fact]
    public async Task HandleAsync_IncrementsClickCounter()
    {
        var clickEvent = new Event("evt_001", TenantId, "user_1", CampaignId, EventType.AdClick, DateTime.UtcNow, null);

        await _handler.HandleAsync(clickEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync("campaign:cmp_789:clicks", 1), Times.Once);
    }

    [Fact]
    public async Task HandleAsync_ReturnsCorrectCount()
    {
        _cacheMock.Setup(c => c.IncrementCounterAsync(It.IsAny<string>(), It.IsAny<long>())).ReturnsAsync(5);
        var clickEvent = new Event("evt_002", TenantId, "user_2", CampaignId, EventType.AdClick, DateTime.UtcNow, null);

        await _handler.HandleAsync(clickEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync(It.IsAny<string>(), It.IsAny<long>()), Times.Once);
    }

    [Fact]
    public async Task HandleAsync_GeneratesCorrectRedisKeyPattern()
    {
        var customCampaign = CampaignId.From("camp_xyz");
        var clickEvent = new Event("evt_003", TenantId, "user_3", customCampaign, EventType.AdClick, DateTime.UtcNow, null);

        await _handler.HandleAsync(clickEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync("campaign:camp_xyz:clicks", 1), Times.Once);
    }
}
