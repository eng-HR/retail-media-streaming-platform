using Microsoft.EntityFrameworkCore;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Infrastructure.Persistence.Repositories;

public class EventRepository : IEventRepository
{
    private readonly AppDbContext _db;

    public EventRepository(AppDbContext db) => _db = db;

    public async Task AddAsync(Event @event, CancellationToken ct = default)
    {
        await _db.Events.AddAsync(@event, ct);
        await _db.SaveChangesAsync(ct);
    }

    public async Task<IReadOnlyList<Event>> GetByCampaignAsync(
        CampaignId campaignId, TenantId tenantId,
        DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        var query = _db.Events
            .Where(e => e.TenantId == tenantId && e.CampaignId == campaignId);

        if (from.HasValue) query = query.Where(e => e.Timestamp >= from.Value);
        if (to.HasValue) query = query.Where(e => e.Timestamp <= to.Value);

        return await query.OrderByDescending(e => e.Timestamp).ToListAsync(ct);
    }
}
