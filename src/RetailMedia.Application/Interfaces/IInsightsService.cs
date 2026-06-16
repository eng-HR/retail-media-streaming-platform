using RetailMedia.Application.DTOs;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Application.Interfaces;

public interface IInsightsService
{
    Task<ClickResponse> GetClicksAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default);
    Task<ImpressionResponse> GetImpressionsAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default);
    Task<ClickToBasketResponse> GetClickToBasketAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default);
    Task<MetricsResponse> GetMetricsAsync(CampaignId campaignId, TenantId tenantId, string? metric = null,
        DateTime? startDate = null, DateTime? endDate = null, CancellationToken ct = default);
}
