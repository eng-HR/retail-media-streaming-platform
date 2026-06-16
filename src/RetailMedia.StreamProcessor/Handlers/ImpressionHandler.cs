using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;

namespace RetailMedia.StreamProcessor.Handlers;

public class ImpressionHandler
{
    private readonly IRedisCache _cache;
    private readonly IMetricsRepository _metricsRepo;
    private readonly ILogger<ImpressionHandler> _logger;

    public ImpressionHandler(IRedisCache cache, IMetricsRepository metricsRepo, ILogger<ImpressionHandler> logger)
    {
        _cache = cache;
        _metricsRepo = metricsRepo;
        _logger = logger;
    }

    public async Task HandleAsync(Event @event)
    {
        var redisKey = $"campaign:{@event.CampaignId}:impressions";
        var count = await _cache.IncrementCounterAsync(redisKey);

        var metric = new CampaignMetric(@event.TenantId, @event.CampaignId, MetricType.Impressions, 1, @event.Timestamp.Date);
        await _metricsRepo.UpsertMetricAsync(metric);

        _logger.LogInformation("Impression event {EventId} for campaign {CampaignId}: total {Count}",
            @event.EventId, @event.CampaignId, count);
    }
}
