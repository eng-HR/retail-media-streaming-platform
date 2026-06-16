using RetailMedia.Application.DTOs;

namespace RetailMedia.Application.Interfaces;

public interface IEventIngestionService
{
    Task IngestAsync(IngestEventRequest request, CancellationToken ct = default);
}
