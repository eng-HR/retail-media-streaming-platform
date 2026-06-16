using Dapper;
using Microsoft.EntityFrameworkCore;
using Npgsql;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Infrastructure.Persistence.Repositories;

public class MetricsRepository : IMetricsRepository
{
    private readonly AppDbContext _db;
    private readonly string _connectionString;

    public MetricsRepository(AppDbContext db, string connectionString)
    {
        _db = db;
        _connectionString = connectionString;
    }

    public async Task<long> GetClickCountAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<long>(
            "SELECT COALESCE(SUM(\"Count\"), 0) FROM \"CampaignMetrics\" WHERE \"TenantId\" = @t AND \"CampaignId\" = @c AND \"Metric\" = 'Clicks'",
            new { t = tenantId.Value, c = campaignId.Value });
    }

    public async Task<long> GetImpressionCountAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<long>(
            "SELECT COALESCE(SUM(\"Count\"), 0) FROM \"CampaignMetrics\" WHERE \"TenantId\" = @t AND \"CampaignId\" = @c AND \"Metric\" = 'Impressions'",
            new { t = tenantId.Value, c = campaignId.Value });
    }

    public async Task<long> GetClickToBasketCountAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_connectionString);
        return await conn.ExecuteScalarAsync<long>(
            "SELECT COALESCE(SUM(\"Count\"), 0) FROM \"CampaignMetrics\" WHERE \"TenantId\" = @t AND \"CampaignId\" = @c AND \"Metric\" = 'ClickToBasket'",
            new { t = tenantId.Value, c = campaignId.Value });
    }

    public async Task UpsertMetricAsync(CampaignMetric metric, CancellationToken ct = default)
    {
        var existing = await _db.CampaignMetrics
            .Where(m => m.TenantId == metric.TenantId
                     && m.CampaignId == metric.CampaignId
                     && m.Metric == metric.Metric
                     && m.Date == metric.Date)
            .FirstOrDefaultAsync(ct);

        if (existing != null)
        {
            existing.Increment(metric.Count);
        }
        else
        {
            await _db.CampaignMetrics.AddAsync(metric, ct);
        }

        await _db.SaveChangesAsync(ct);
    }

    public async Task<IReadOnlyList<CampaignMetric>> GetMetricsAsync(
        CampaignId campaignId, TenantId tenantId, MetricType? metric = null,
        DateTime? from = null, DateTime? to = null, CancellationToken ct = default)
    {
        var query = _db.CampaignMetrics
            .Where(m => m.TenantId == tenantId && m.CampaignId == campaignId);

        if (metric.HasValue) query = query.Where(m => m.Metric == metric.Value);
        if (from.HasValue) query = query.Where(m => m.Date >= from.Value.ToUniversalTime());
        if (to.HasValue) query = query.Where(m => m.Date <= to.Value.ToUniversalTime());

        return await query.ToListAsync(ct);
    }
}
