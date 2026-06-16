using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Application.DTOs;

public record IngestEventRequest(
    string EventId,
    string TenantId,
    string UserId,
    string CampaignId,
    string EventType,
    DateTime Timestamp,
    Dictionary<string, string>? Metadata = null);
