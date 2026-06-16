using RetailMedia.Domain.Entities;

namespace RetailMedia.Application.DTOs;

public record MetricsResponse(
    string CampaignId,
    long? Clicks,
    long? Impressions,
    long? ClickToBasket,
    DateTime? StartDate,
    DateTime? EndDate);
