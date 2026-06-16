using RetailMedia.Application.DTOs;
using RetailMedia.Application.Interfaces;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Application.Services;

public class EventIngestionService : IEventIngestionService
{
    private readonly IKafkaProducer _kafkaProducer;
    private readonly IEventRepository _eventRepo;

    public EventIngestionService(IKafkaProducer kafkaProducer, IEventRepository eventRepo)
    {
        _kafkaProducer = kafkaProducer;
        _eventRepo = eventRepo;
    }

    public async Task IngestAsync(IngestEventRequest request, CancellationToken ct = default)
    {
        if (!Enum.TryParse<EventType>(request.EventType, ignoreCase: true, out var eventType))
            throw new ArgumentException($"Invalid event type: {request.EventType}");

        var @event = new Event(
            request.EventId,
            TenantId.From(request.TenantId),
            request.UserId,
            CampaignId.From(request.CampaignId),
            eventType,
            request.Timestamp,
            request.Metadata);

        await _kafkaProducer.PublishAsync(@event, ct);
    }
}
