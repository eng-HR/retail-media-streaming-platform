using Microsoft.Extensions.Logging;
using Moq;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;
using RetailMedia.StreamProcessor.Handlers;

namespace RetailMedia.StreamProcessor.Tests;

public class ImpressionHandlerTests
{
    private readonly Mock<IRedisCache> _cacheMock;
    private readonly Mock<IMetricsRepository> _metricsRepoMock;
    private readonly ImpressionHandler _handler;
    private static readonly TenantId TenantId = TenantId.From("tesco");
    private static readonly CampaignId CampaignId = CampaignId.From("cmp_789");

    public ImpressionHandlerTests()
    {
        _cacheMock = new Mock<IRedisCache>();
        _metricsRepoMock = new Mock<IMetricsRepository>();
        var loggerMock = new Mock<ILogger<ImpressionHandler>>();
        _handler = new ImpressionHandler(_cacheMock.Object, _metricsRepoMock.Object, loggerMock.Object);
    }

    [Fact]
    public async Task HandleAsync_IncrementsImpressionCounter()
    {
        var impressionEvent = new Event("evt_010", TenantId, "user_1", CampaignId, EventType.AdImpression, DateTime.UtcNow, null);

        await _handler.HandleAsync(impressionEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync("campaign:cmp_789:impressions", 1), Times.Once);
    }

    [Fact]
    public async Task HandleAsync_UsesCorrectRedisKey()
    {
        var customCampaign = CampaignId.From("camp_xyz");
        var impressionEvent = new Event("evt_011", TenantId, "user_2", customCampaign, EventType.AdImpression, DateTime.UtcNow, null);

        await _handler.HandleAsync(impressionEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync("campaign:camp_xyz:impressions", 1), Times.Once);
    }

    [Fact]
    public async Task HandleAsync_MultipleEvents_AccumulatesCount()
    {
        _cacheMock.SetupSequence(c => c.IncrementCounterAsync(It.IsAny<string>(), It.IsAny<long>()))
            .ReturnsAsync(1)
            .ReturnsAsync(2)
            .ReturnsAsync(3);

        var evt = new Event("evt_012", TenantId, "user_3", CampaignId, EventType.AdImpression, DateTime.UtcNow, null);

        await _handler.HandleAsync(evt);
        await _handler.HandleAsync(new Event("evt_013", TenantId, "user_3", CampaignId, EventType.AdImpression, DateTime.UtcNow, null));
        await _handler.HandleAsync(new Event("evt_014", TenantId, "user_3", CampaignId, EventType.AdImpression, DateTime.UtcNow, null));

        _cacheMock.Verify(c => c.IncrementCounterAsync(It.IsAny<string>(), It.IsAny<long>()), Times.Exactly(3));
    }
}
