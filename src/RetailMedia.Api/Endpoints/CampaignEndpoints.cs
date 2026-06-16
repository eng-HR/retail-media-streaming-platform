using RetailMedia.Application.DTOs;
using RetailMedia.Application.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Api.Endpoints;

public static class CampaignEndpoints
{
    public static void MapCampaignEndpoints(this WebApplication app)
    {
        var group = app.MapGroup("/ad/{campaignId}")
            .WithTags("Campaigns");

        group.MapGet("/clicks", GetClicks);
        group.MapGet("/impressions", GetImpressions);
        group.MapGet("/clickToBasket", GetClickToBasket);
        group.MapGet("/metrics", GetMetrics);
    }

    static async Task<IResult> GetClicks(
        string campaignId,
        IInsightsService service,
        ITenantContext tenant,
        CancellationToken ct)
    {
        var result = await service.GetClicksAsync(
            CampaignId.From(campaignId), tenant.CurrentTenantId, ct);
        return Results.Ok(new { data = result, meta = new { timestamp = DateTime.UtcNow } });
    }

    static async Task<IResult> GetImpressions(
        string campaignId,
        IInsightsService service,
        ITenantContext tenant,
        CancellationToken ct)
    {
        var result = await service.GetImpressionsAsync(
            CampaignId.From(campaignId), tenant.CurrentTenantId, ct);
        return Results.Ok(new { data = result, meta = new { timestamp = DateTime.UtcNow } });
    }

    static async Task<IResult> GetClickToBasket(
        string campaignId,
        IInsightsService service,
        ITenantContext tenant,
        CancellationToken ct)
    {
        var result = await service.GetClickToBasketAsync(
            CampaignId.From(campaignId), tenant.CurrentTenantId, ct);
        return Results.Ok(new { data = result, meta = new { timestamp = DateTime.UtcNow } });
    }

    static async Task<IResult> GetMetrics(
        string campaignId,
        string? metric,
        DateTime? startDate,
        DateTime? endDate,
        IInsightsService service,
        ITenantContext tenant,
        CancellationToken ct)
    {
        var result = await service.GetMetricsAsync(
            CampaignId.From(campaignId), tenant.CurrentTenantId,
            metric, startDate, endDate, ct);
        return Results.Ok(new { data = result, meta = new { timestamp = DateTime.UtcNow } });
    }
}
