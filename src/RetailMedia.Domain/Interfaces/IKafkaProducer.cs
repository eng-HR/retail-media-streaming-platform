using RetailMedia.Domain.Entities;

namespace RetailMedia.Domain.Interfaces;

public interface IKafkaProducer
{
    Task PublishAsync(Event @event, CancellationToken ct = default);
}
