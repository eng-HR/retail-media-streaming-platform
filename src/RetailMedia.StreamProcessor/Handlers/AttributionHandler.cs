using System.Globalization;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;

namespace RetailMedia.StreamProcessor.Handlers;

public class AttributionHandler
{
    private readonly IRedisCache _cache;
    private readonly IMetricsRepository _metricsRepo;
    private readonly ILogger<AttributionHandler> _logger;
    private static readonly TimeSpan AttributionWindow = TimeSpan.FromMinutes(30);

    public AttributionHandler(IRedisCache cache, IMetricsRepository metricsRepo, ILogger<AttributionHandler> logger)
    {
        _cache = cache;
        _metricsRepo = metricsRepo;
        _logger = logger;
    }

    public async Task HandleClickAsync(Event @event)
    {
        var sessionKey = $"session:{@event.TenantId}:{@event.UserId}";
        var session = new Dictionary<string, string>
        {
            ["campaignId"] = @event.CampaignId.ToString(),
            ["timestamp"] = DateTime.UtcNow.ToString("O")
        };
        await _cache.SetAsync(sessionKey, session, AttributionWindow);
        _logger.LogInformation("Stored click session for user {UserId} campaign {CampaignId}",
            @event.UserId, @event.CampaignId);
    }

    public async Task HandleAddToCartAsync(Event @event)
    {
        var sessionKey = $"session:{@event.TenantId}:{@event.UserId}";
        var session = await _cache.GetAsync<Dictionary<string, string>>(sessionKey);

        if (session == null) return;

        if (!DateTime.TryParse(session["timestamp"], CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal, out var clickTime)) return;
        if (DateTime.UtcNow - clickTime > AttributionWindow)
        {
            _logger.LogInformation("Attribution window expired for user {UserId}", @event.UserId);
            return;
        }

        var redisKey = $"campaign:{@event.CampaignId}:clickToBasket";
        await _cache.IncrementCounterAsync(redisKey, expiry: TimeSpan.FromHours(24));

        var metric = new CampaignMetric(@event.TenantId, @event.CampaignId, MetricType.ClickToBasket, 1, @event.Timestamp.Date);
        await _metricsRepo.UpsertMetricAsync(metric);

        _logger.LogInformation("Attributed add-to-cart for user {UserId} campaign {CampaignId}",
            @event.UserId, @event.CampaignId);
    }
}
