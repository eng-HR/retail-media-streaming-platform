using System.Text.Json;
using Confluent.Kafka;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Domain.ValueObjects;
using RetailMedia.StreamProcessor.Handlers;

namespace RetailMedia.StreamProcessor;

public class KafkaEventConsumer : BackgroundService
{
    private readonly IConsumer<string, string> _consumer;
    private readonly IServiceProvider _services;
    private readonly ILogger<KafkaEventConsumer> _logger;
    private const string Topic = "raw-events";

    public KafkaEventConsumer(string bootstrapServers, IServiceProvider services, ILogger<KafkaEventConsumer> logger)
    {
        _services = services;
        _logger = logger;
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = "retail-media-processor",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
        _consumer = new ConsumerBuilder<string, string>(config).Build();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
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
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task ProcessMessageAsync(Message<string, string> message, CancellationToken ct)
    {
        using var scope = _services.CreateScope();
        var cache = scope.ServiceProvider.GetRequiredService<IRedisCache>();
        var metricsRepo = scope.ServiceProvider.GetRequiredService<IMetricsRepository>();
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
        _consumer?.Close();
        _consumer?.Dispose();
        base.Dispose();
    }
}
