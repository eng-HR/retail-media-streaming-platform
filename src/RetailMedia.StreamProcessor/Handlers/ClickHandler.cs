using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.StreamProcessor.Handlers;

public class ClickHandler
{
    private readonly IRedisCache _cache;
    private readonly ILogger<ClickHandler> _logger;

    public ClickHandler(IRedisCache cache, ILogger<ClickHandler> logger)
    {
        _cache = cache;
        _logger = logger;
    }

    public async Task HandleAsync(Event @event)
    {
        var redisKey = $"campaign:{@event.CampaignId}:clicks";
        var count = await _cache.IncrementCounterAsync(redisKey);
        _logger.LogInformation("Click event {EventId} for campaign {CampaignId}: total {Count}",
            @event.EventId, @event.CampaignId, count);
    }
}
