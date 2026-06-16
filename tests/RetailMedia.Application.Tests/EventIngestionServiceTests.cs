using Moq;
using RetailMedia.Application.DTOs;
using RetailMedia.Application.Interfaces;
using RetailMedia.Application.Services;
using RetailMedia.Domain.Interfaces;

namespace RetailMedia.Application.Tests;

public class EventIngestionServiceTests
{
    private readonly Mock<IKafkaProducer> _kafkaMock;
    private readonly Mock<IEventRepository> _eventRepoMock;
    private readonly IEventIngestionService _service;

    public EventIngestionServiceTests()
    {
        _kafkaMock = new Mock<IKafkaProducer>();
        _eventRepoMock = new Mock<IEventRepository>();
        _service = new EventIngestionService(_kafkaMock.Object, _eventRepoMock.Object);
    }

    [Fact]
    public async Task IngestAsync_ValidEvent_PublishesToKafka()
    {
        var request = new IngestEventRequest(
            "evt_100", "tesco", "user_1", "cmp_789",
            "AdImpression", DateTime.UtcNow);

        await _service.IngestAsync(request);

        _kafkaMock.Verify(p => p.PublishAsync(It.Is<Domain.Entities.Event>(e =>
            e.EventId == "evt_100" &&
            e.TenantId.ToString() == "tesco" &&
            e.CampaignId.ToString() == "cmp_789" &&
            e.Type == Domain.ValueObjects.EventType.AdImpression
        ), default), Times.Once);
    }

    [Fact]
    public async Task IngestAsync_ValidEvent_DoesNotCallEventRepo()
    {
        var request = new IngestEventRequest(
            "evt_101", "tesco", "user_1", "cmp_789",
            "AdClick", DateTime.UtcNow);

        await _service.IngestAsync(request);

        _eventRepoMock.Verify(r => r.AddAsync(It.IsAny<Domain.Entities.Event>(), default), Times.Never);
    }

    [Fact]
    public async Task IngestAsync_EventTypeCaseInsensitive_Works()
    {
        var request = new IngestEventRequest(
            "evt_102", "tesco", "user_1", "cmp_789",
            "adimpression", DateTime.UtcNow);

        await _service.IngestAsync(request);

        _kafkaMock.Verify(p => p.PublishAsync(It.Is<Domain.Entities.Event>(e =>
            e.Type == Domain.ValueObjects.EventType.AdImpression
        ), default), Times.Once);
    }

    [Fact]
    public async Task IngestAsync_EventTypeDifferentCase_Works()
    {
        var request = new IngestEventRequest(
            "evt_103", "tesco", "user_1", "cmp_789",
            "adclick", DateTime.UtcNow);

        await _service.IngestAsync(request);

        _kafkaMock.Verify(p => p.PublishAsync(It.Is<Domain.Entities.Event>(e =>
            e.Type == Domain.ValueObjects.EventType.AdClick
        ), default), Times.Once);
    }

    [Fact]
    public async Task IngestAsync_InvalidEventType_ThrowsArgumentException()
    {
        var request = new IngestEventRequest(
            "evt_104", "tesco", "user_1", "cmp_789",
            "BogusType", DateTime.UtcNow);

        await Assert.ThrowsAsync<ArgumentException>(() => _service.IngestAsync(request));

        _kafkaMock.Verify(p => p.PublishAsync(It.IsAny<Domain.Entities.Event>(), default), Times.Never);
    }

    [Fact]
    public async Task IngestAsync_EmptyEventType_ThrowsArgumentException()
    {
        var request = new IngestEventRequest(
            "evt_105", "tesco", "user_1", "cmp_789",
            "", DateTime.UtcNow);

        await Assert.ThrowsAsync<ArgumentException>(() => _service.IngestAsync(request));
    }

    [Fact]
    public async Task IngestAsync_NullTenantId_ThrowsArgumentNullException()
    {
        var request = new IngestEventRequest(
            "evt_106", "", "user_1", "cmp_789",
            "AdImpression", DateTime.UtcNow);

        await Assert.ThrowsAsync<ArgumentException>(() => _service.IngestAsync(request));
    }

    [Fact]
    public async Task IngestAsync_NullCampaignId_ThrowsArgumentNullException()
    {
        var request = new IngestEventRequest(
            "evt_107", "tesco", "user_1", "",
            "AdImpression", DateTime.UtcNow);

        await Assert.ThrowsAsync<ArgumentException>(() => _service.IngestAsync(request));
    }

    [Fact]
    public async Task IngestAsync_WithMetadata_PublishesWithMetadata()
    {
        var metadata = new Dictionary<string, string>
        {
            ["productId"] = "prod_123",
            ["source"] = "homepage_banner"
        };
        var request = new IngestEventRequest(
            "evt_108", "tesco", "user_1", "cmp_789",
            "ProductView", DateTime.UtcNow, metadata);

        await _service.IngestAsync(request);

        _kafkaMock.Verify(p => p.PublishAsync(It.Is<Domain.Entities.Event>(e =>
            e.Metadata != null &&
            e.Metadata["productId"] == "prod_123" &&
            e.Metadata["source"] == "homepage_banner"
        ), default), Times.Once);
    }
}
