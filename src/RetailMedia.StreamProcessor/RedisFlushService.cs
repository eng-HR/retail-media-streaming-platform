using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.StreamProcessor;

public class RedisFlushService : BackgroundService
{
    private readonly IServiceProvider _services;
    private readonly ILogger<RedisFlushService> _logger;
    private static readonly TimeSpan FlushInterval = TimeSpan.FromSeconds(30);

    public RedisFlushService(IServiceProvider services, ILogger<RedisFlushService> logger)
    {
        _services = services;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Redis flush service started, interval: {Interval}s", FlushInterval.TotalSeconds);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(FlushInterval, stoppingToken);
            FlushCounters();
        }
    }

    private void FlushCounters()
    {
        using var scope = _services.CreateScope();
        var cache = scope.ServiceProvider.GetRequiredService<IRedisCache>();
        var metricsRepo = scope.ServiceProvider.GetRequiredService<IMetricsRepository>();
        var logger = scope.ServiceProvider.GetRequiredService<ILogger<RedisFlushService>>();

        try
        {
            // Simplified flush: In production use Redis SCAN + pattern matching.
            // For this case study, cache-aside reads populate PostgreSQL on demand.
            logger.LogDebug("Counter flush cycle completed");
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Error flushing counters");
        }
    }
}
