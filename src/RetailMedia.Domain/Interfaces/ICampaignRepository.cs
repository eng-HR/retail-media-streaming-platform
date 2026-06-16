using RetailMedia.Domain.Entities;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Domain.Interfaces;

public interface ICampaignRepository
{
    Task<Campaign?> GetByIdAsync(CampaignId id, TenantId tenantId, CancellationToken ct = default);
    Task<IReadOnlyList<Campaign>> GetByTenantAsync(TenantId tenantId, CancellationToken ct = default);
    Task AddAsync(Campaign campaign, CancellationToken ct = default);
    Task UpdateAsync(Campaign campaign, CancellationToken ct = default);
}
