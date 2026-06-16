using RetailMedia.Domain.Entities;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Domain.Interfaces;

public interface IMetricsRepository
{
    Task<long> GetClickCountAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default);
    Task<long> GetImpressionCountAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default);
    Task<long> GetClickToBasketCountAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default);
    Task UpsertMetricAsync(CampaignMetric metric, CancellationToken ct = default);
    Task<IReadOnlyList<CampaignMetric>> GetMetricsAsync(
        CampaignId campaignId, TenantId tenantId, MetricType? metric = null,
        DateTime? from = null, DateTime? to = null, CancellationToken ct = default);
}
