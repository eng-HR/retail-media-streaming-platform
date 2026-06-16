using RetailMedia.Domain.Entities;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Domain.Interfaces;

public interface IEventRepository
{
    Task AddAsync(Event @event, CancellationToken ct = default);
    Task<IReadOnlyList<Event>> GetByCampaignAsync(
        CampaignId campaignId, TenantId tenantId, DateTime? from = null, DateTime? to = null, CancellationToken ct = default);
}
