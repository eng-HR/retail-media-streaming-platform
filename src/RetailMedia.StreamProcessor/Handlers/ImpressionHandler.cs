using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;

namespace RetailMedia.StreamProcessor.Handlers;

public class ImpressionHandler
{
    private readonly IRedisCache _cache;
    private readonly ILogger<ImpressionHandler> _logger;

    public ImpressionHandler(IRedisCache cache, ILogger<ImpressionHandler> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task HandleAsync(Event @event)
    {
        var redisKey = $"campaign:{@event.CampaignId}:impressions";
        var count = await _cache.IncrementCounterAsync(redisKey);
        _logger.LogInformation("Impression event {EventId} for campaign {CampaignId}: total {Count}",
            @event.EventId, @event.CampaignId, count);
    }
}
