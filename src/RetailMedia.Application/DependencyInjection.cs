using Microsoft.Extensions.DependencyInjection;
using RetailMedia.Application.Interfaces;
using RetailMedia.Application.Services;

namespace RetailMedia.Application;

public static class DependencyInjection
{
    public static IServiceCollection AddApplication(this IServiceCollection services)
    {
        services.AddScoped<IInsightsService, InsightsService>();
        services.AddScoped<IEventIngestionService, EventIngestionService>();
        return services;
    }
}
