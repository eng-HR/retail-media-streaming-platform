using RetailMedia.Application.DTOs;
using RetailMedia.Application.Interfaces;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Application.Services;

public class InsightsService : IInsightsService
{
    private readonly IRedisCache _cache;
    private readonly IMetricsRepository _metricsRepo;

    public InsightsService(IRedisCache cache, IMetricsRepository metricsRepo)
    {
        _cache = cache;
        _metricsRepo = metricsRepo;
    }

    public async Task<ClickResponse> GetClicksAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default)
    {
        var redisKey = $"campaign:{campaignId}:clicks";
        var redisCount = await _cache.GetCounterAsync(redisKey);
        var dbCount = await _metricsRepo.GetClickCountAsync(campaignId, tenantId, ct);
        return new ClickResponse(campaignId.ToString(), redisCount + dbCount, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
    }

    public async Task<ImpressionResponse> GetImpressionsAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default)
    {
        var redisKey = $"campaign:{campaignId}:impressions";
        var redisCount = await _cache.GetCounterAsync(redisKey);
        var dbCount = await _metricsRepo.GetImpressionCountAsync(campaignId, tenantId, ct);
        return new ImpressionResponse(campaignId.ToString(), redisCount + dbCount, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
    }

    public async Task<ClickToBasketResponse> GetClickToBasketAsync(CampaignId campaignId, TenantId tenantId, CancellationToken ct = default)
    {
        var redisKey = $"campaign:{campaignId}:clickToBasket";
        var redisCount = await _cache.GetCounterAsync(redisKey);
        var dbCount = await _metricsRepo.GetClickToBasketCountAsync(campaignId, tenantId, ct);
        return new ClickToBasketResponse(campaignId.ToString(), redisCount + dbCount, DateTimeOffset.UtcNow.ToUnixTimeSeconds());
    }

    public async Task<MetricsResponse> GetMetricsAsync(CampaignId campaignId, TenantId tenantId,
        string? metric = null, DateTime? startDate = null, DateTime? endDate = null, CancellationToken ct = default)
    {
        var metricType = metric?.ToLowerInvariant() switch
        {
            "clicks" => MetricType.Clicks,
            "impressions" => MetricType.Impressions,
            "clicktobasket" => MetricType.ClickToBasket,
            _ => (MetricType?)null
        };

        var metrics = await _metricsRepo.GetMetricsAsync(campaignId, tenantId, metricType,
            startDate?.Date, endDate?.Date, ct);

        long? clicks = null, impressions = null, clickToBasket = null;

        foreach (var m in metrics)
        {
            switch (m.Metric)
            {
                case MetricType.Clicks:
                    clicks = (clicks ?? 0) + m.Count;
                    break;
                case MetricType.Impressions:
                    impressions = (impressions ?? 0) + m.Count;
                    break;
                case MetricType.ClickToBasket:
                    clickToBasket = (clickToBasket ?? 0) + m.Count;
                    break;
            }
        }

        return new MetricsResponse(
            campaignId.ToString(), clicks, impressions, clickToBasket, startDate, endDate);
    }
}
