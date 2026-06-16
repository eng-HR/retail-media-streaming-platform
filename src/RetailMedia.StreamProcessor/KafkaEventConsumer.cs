using System.Text.Json;
using Confluent.Kafka;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;
using RetailMedia.StreamProcessor.Handlers;

namespace RetailMedia.StreamProcessor;

public class KafkaEventConsumer : BackgroundService
{
    private readonly string _bootstrapServers;
    private readonly IServiceProvider _services;
    private readonly ILogger<KafkaEventConsumer> _logger;
    private IConsumer<string, string> _consumer;
    private const string Topic = "raw-events";

    public KafkaEventConsumer(string bootstrapServers, IServiceProvider services, ILogger<KafkaEventConsumer> logger)
    {
        _bootstrapServers = bootstrapServers;
        _services = services;
        _logger = logger;
        _consumer = CreateConsumer();
    }

    private IConsumer<string, string> CreateConsumer()
    {
        var old = _consumer;
        if (old != null)
        {
            try { old.Close(); } catch { }
            try { old.Dispose(); } catch { }
        }

        return new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = "retail-media-processor",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            AllowAutoCreateTopics = true
        }).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                _consumer.Subscribe(Topic);
                _logger.LogInformation("Started consuming topic {Topic}", Topic);

                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = _consumer.Consume(stoppingToken);
                        await ProcessMessageAsync(result.Message, stoppingToken);
                        _consumer.Commit(result);
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Kafka consume error");
                        break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Consumer faulted, recreating...");
            }

            if (!stoppingToken.IsCancellationRequested)
            {
                _consumer = CreateConsumer();
                await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
            }
        }
    }

    private async Task ProcessMessageAsync(Message<string, string> message, CancellationToken ct)
    {
        using var scope = _services.CreateScope();
        var cache = scope.ServiceProvider.GetRequiredService<IRedisCache>();
        var loggerFactory = scope.ServiceProvider.GetRequiredService<ILoggerFactory>();

        try
        {
            var doc = JsonDocument.Parse(message.Value);
            var root = doc.RootElement;

            var eventType = root.GetProperty("eventType").GetString() ?? "";
            var @event = new Event(
                root.GetProperty("eventId").GetString()!,
                TenantId.From(root.GetProperty("tenantId").GetString()!),
                root.GetProperty("userId").GetString()!,
                CampaignId.From(root.GetProperty("campaignId").GetString()!),
                Enum.Parse<EventType>(eventType, ignoreCase: true),
                root.GetProperty("timestamp").GetDateTime(),
                null);

            switch (@event.Type)
            {
                case EventType.AdClick:
                    await new ClickHandler(cache, loggerFactory.CreateLogger<ClickHandler>()).HandleAsync(@event);
                    await new AttributionHandler(cache, loggerFactory.CreateLogger<AttributionHandler>()).HandleClickAsync(@event);
                    break;

                case EventType.AdImpression:
                    await new ImpressionHandler(cache, loggerFactory.CreateLogger<ImpressionHandler>()).HandleAsync(@event);
                    break;

                case EventType.AddToCart:
                    await new AttributionHandler(cache, loggerFactory.CreateLogger<AttributionHandler>()).HandleAddToCartAsync(@event);
                    break;

                default:
                    _logger.LogDebug("Unhandled event type: {Type}", @event.Type);
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error processing message {Key}", message.Key);
        }
    }

    public override void Dispose()
    {
        try { _consumer?.Close(); } catch { }
        try { _consumer?.Dispose(); } catch { }
        base.Dispose();
    }
}
