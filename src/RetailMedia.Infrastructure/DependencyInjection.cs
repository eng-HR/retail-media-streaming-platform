using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RetailMedia.Domain.Interfaces;
using RetailMedia.Infrastructure.Caching;
using RetailMedia.Infrastructure.Messaging;
using RetailMedia.Infrastructure.Persistence;
using RetailMedia.Infrastructure.Persistence.Repositories;

namespace RetailMedia.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInfrastructure(this IServiceCollection services, IConfiguration config)
    {
        var connectionString = config.GetConnectionString("DefaultConnection")
            ?? "Host=localhost;Port=5433;Database=retail_media;Username=retail;Password=retail123";

        var redisConnection = config["Redis:ConnectionString"] ?? "localhost:6380";
        var kafkaBootstrapServers = config["Kafka:BootstrapServers"] ?? "localhost:9092";

        services.AddDbContext<AppDbContext>(options =>
            options.UseNpgsql(connectionString));

        services.AddScoped<ICampaignRepository, CampaignRepository>();
        services.AddScoped<IEventRepository, EventRepository>();
        services.AddSingleton<IRedisCache>(_ => new RedisCache(redisConnection));
        services.AddScoped<IMetricsRepository>(sp =>
        {
            var db = sp.GetRequiredService<AppDbContext>();
            return new MetricsRepository(db, connectionString);
        });
        services.AddSingleton<IKafkaProducer>(sp =>
        {
            var logger = sp.GetRequiredService<ILogger<KafkaProducer>>();
            return new KafkaProducer(kafkaBootstrapServers, logger);
        });

        return services;
    }
}
