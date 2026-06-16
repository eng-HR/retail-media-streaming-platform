using Microsoft.EntityFrameworkCore;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Infrastructure.Persistence.Repositories;

public class CampaignRepository : ICampaignRepository
{
    private readonly AppDbContext _db;

    public CampaignRepository(AppDbContext db) => _db = db;

    public async Task<Campaign?> GetByIdAsync(CampaignId id, TenantId tenantId, CancellationToken ct = default) =>
        await _db.Campaigns
            .Where(c => c.Id == id && c.TenantId == tenantId)
            .FirstOrDefaultAsync(ct);

    public async Task<IReadOnlyList<Campaign>> GetByTenantAsync(TenantId tenantId, CancellationToken ct = default) =>
        await _db.Campaigns
            .Where(c => c.TenantId == tenantId)
            .ToListAsync(ct);

    public async Task AddAsync(Campaign campaign, CancellationToken ct = default)
    {
        await _db.Campaigns.AddAsync(campaign, ct);
        await _db.SaveChangesAsync(ct);
    }

    public async Task UpdateAsync(Campaign campaign, CancellationToken ct = default)
    {
        _db.Campaigns.Update(campaign);
        await _db.SaveChangesAsync(ct);
    }
}
