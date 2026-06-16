using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Domain.Entities;

public class Campaign
{
    public CampaignId Id { get; private set; } = null!;
    public TenantId TenantId { get; private set; } = null!;
    public string Name { get; private set; } = string.Empty;
    public bool IsActive { get; private set; }
    public DateTime CreatedAt { get; private set; }
    public DateTime UpdatedAt { get; private set; }

    private Campaign() { }

    public Campaign(CampaignId id, TenantId tenantId, string name)
    {
        Id = id ?? throw new ArgumentNullException(nameof(id));
        TenantId = tenantId ?? throw new ArgumentNullException(nameof(tenantId));
        Name = name ?? throw new ArgumentNullException(nameof(name));
        IsActive = true;
        CreatedAt = DateTime.UtcNow;
        UpdatedAt = DateTime.UtcNow;
    }

    public void Deactivate()
    {
        IsActive = false;
        UpdatedAt = DateTime.UtcNow;
    }

    public void Rename(string newName)
    {
        Name = newName ?? throw new ArgumentNullException(nameof(newName));
        UpdatedAt = DateTime.UtcNow;
    }
}
