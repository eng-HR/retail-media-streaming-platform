using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Domain.Entities;

public class Event
{
    public string EventId { get; private set; } = string.Empty;
    public TenantId TenantId { get; private set; } = null!;
    public string UserId { get; private set; } = string.Empty;
    public CampaignId CampaignId { get; private set; } = null!;
    public EventType Type { get; private set; }
    public DateTime Timestamp { get; private set; }
    public Dictionary<string, string>? Metadata { get; private set; }

    private Event() { }

    public Event(
        string eventId,
        TenantId tenantId,
        string userId,
        CampaignId campaignId,
        EventType type,
        DateTime timestamp,
        Dictionary<string, string>? metadata = null)
    {
        EventId = eventId ?? throw new ArgumentNullException(nameof(eventId));
        TenantId = tenantId ?? throw new ArgumentNullException(nameof(tenantId));
        UserId = userId ?? throw new ArgumentNullException(nameof(userId));
        CampaignId = campaignId ?? throw new ArgumentNullException(nameof(campaignId));
        Type = type;
        Timestamp = timestamp;
        Metadata = metadata;
    }
}
