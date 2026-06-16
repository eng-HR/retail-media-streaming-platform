using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;

namespace RetailMedia.Infrastructure.Messaging;

public class KafkaProducer : IKafkaProducer, IDisposable
{
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaProducer> _logger;
    private const string Topic = "raw-events";

    public KafkaProducer(string bootstrapServers, ILogger<KafkaProducer> logger)
    {
        _logger = logger;
        var config = new ProducerConfig { BootstrapServers = bootstrapServers };
        _producer = new ProducerBuilder<string, string>(config).Build();
    }

    public async Task PublishAsync(Event @event, CancellationToken ct = default)
    {
        var key = $"{@event.TenantId}:{@event.CampaignId}";
        var value = JsonSerializer.Serialize(new
        {
            eventId = @event.EventId,
            tenantId = @event.TenantId.ToString(),
            userId = @event.UserId,
            campaignId = @event.CampaignId.ToString(),
            eventType = @event.Type.ToString(),
            timestamp = @event.Timestamp,
            metadata = @event.Metadata
        });

        var result = await _producer.ProduceAsync(Topic, new Message<string, string>
        {
            Key = key,
            Value = value
        }, ct);

        _logger.LogInformation("Published event {EventId} to topic {Topic} partition {Partition} offset {Offset}",
            @event.EventId, Topic, result.Partition, result.Offset);
    }

    public void Dispose() => _producer?.Dispose();
}
