using Microsoft.Extensions.Logging;
using Moq;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;
using RetailMedia.StreamProcessor.Handlers;

namespace RetailMedia.StreamProcessor.Tests;

public class AttributionHandlerTests
{
    private readonly Mock<IRedisCache> _cacheMock;
    private readonly Mock<IMetricsRepository> _metricsRepoMock;
    private readonly AttributionHandler _handler;
    private static readonly TenantId TenantId = TenantId.From("tesco");
    private static readonly CampaignId CampaignId = CampaignId.From("cmp_789");

    public AttributionHandlerTests()
    {
        _cacheMock = new Mock<IRedisCache>();
        _metricsRepoMock = new Mock<IMetricsRepository>();
        var loggerMock = new Mock<ILogger<AttributionHandler>>();
        _handler = new AttributionHandler(_cacheMock.Object, _metricsRepoMock.Object, loggerMock.Object);
    }

    [Fact]
    public async Task HandleClickAsync_StoresSessionWith30MinTTL()
    {
        var clickEvent = new Event("evt_020", TenantId, "user_1", CampaignId, EventType.AdClick, DateTime.UtcNow, null);

        await _handler.HandleClickAsync(clickEvent);

        _cacheMock.Verify(c => c.SetAsync(
            "session:tesco:user_1",
            It.Is<Dictionary<string, string>>(d => d["campaignId"] == "cmp_789"),
            It.Is<TimeSpan>(t => t == TimeSpan.FromMinutes(30))), Times.Once);
    }

    [Fact]
    public async Task HandleAddToCartAsync_WithActiveSession_Attributed()
    {
        var session = new Dictionary<string, string>
        {
            ["campaignId"] = "cmp_789",
            ["timestamp"] = DateTime.UtcNow.ToString("O")
        };
        _cacheMock.Setup(c => c.GetAsync<Dictionary<string, string>>("session:tesco:user_1"))
            .ReturnsAsync(session);

        var addToCartEvent = new Event("evt_021", TenantId, "user_1", CampaignId, EventType.AddToCart, DateTime.UtcNow, null);

        await _handler.HandleAddToCartAsync(addToCartEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync("campaign:cmp_789:clickToBasket", 1), Times.Once);
    }

    [Fact]
    public async Task HandleAddToCartAsync_WithoutSession_NotAttributed()
    {
        _cacheMock.Setup(c => c.GetAsync<Dictionary<string, string>>("session:tesco:user_2"))
            .ReturnsAsync((Dictionary<string, string>?)null);

        var addToCartEvent = new Event("evt_022", TenantId, "user_2", CampaignId, EventType.AddToCart, DateTime.UtcNow, null);

        await _handler.HandleAddToCartAsync(addToCartEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync(It.IsAny<string>(), It.IsAny<long>()), Times.Never);
    }

    [Fact]
    public async Task HandleAddToCartAsync_WithExpiredSession_NotAttributed()
    {
        var expiredTimestamp = DateTime.UtcNow.AddHours(-2).ToString("O");
        var expiredSession = new Dictionary<string, string>
        {
            ["campaignId"] = "cmp_789",
            ["timestamp"] = expiredTimestamp
        };
        _cacheMock.Setup(c => c.GetAsync<Dictionary<string, string>>("session:tesco:user_3"))
            .ReturnsAsync(expiredSession);

        var addToCartEvent = new Event("evt_023", TenantId, "user_3", CampaignId, EventType.AddToCart, DateTime.UtcNow, null);

        await _handler.HandleAddToCartAsync(addToCartEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync(It.IsAny<string>(), It.IsAny<long>()), Times.Never);
    }

    [Fact]
    public async Task HandleAddToCartAsync_WithInvalidTimestamp_NotAttributed()
    {
        var badSession = new Dictionary<string, string>
        {
            ["campaignId"] = "cmp_789",
            ["timestamp"] = "not-a-date"
        };
        _cacheMock.Setup(c => c.GetAsync<Dictionary<string, string>>("session:tesco:user_4"))
            .ReturnsAsync(badSession);

        var addToCartEvent = new Event("evt_024", TenantId, "user_4", CampaignId, EventType.AddToCart, DateTime.UtcNow, null);

        await _handler.HandleAddToCartAsync(addToCartEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync(It.IsAny<string>(), It.IsAny<long>()), Times.Never);
    }

    [Fact]
    public async Task HandleAddToCartAsync_AttributesToCorrectCampaign()
    {
        var session = new Dictionary<string, string>
        {
            ["campaignId"] = "cmp_other",
            ["timestamp"] = DateTime.UtcNow.ToString("O")
        };
        _cacheMock.Setup(c => c.GetAsync<Dictionary<string, string>>("session:tesco:user_5"))
            .ReturnsAsync(session);

        var addToCartEvent = new Event("evt_025", TenantId, "user_5", CampaignId, EventType.AddToCart, DateTime.UtcNow, null);

        await _handler.HandleAddToCartAsync(addToCartEvent);

        _cacheMock.Verify(c => c.IncrementCounterAsync("campaign:cmp_789:clickToBasket", 1), Times.Once);
    }
}
