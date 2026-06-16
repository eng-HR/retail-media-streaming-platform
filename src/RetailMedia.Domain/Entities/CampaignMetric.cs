using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Domain.Entities;

public class CampaignMetric
{
    public long Id { get; private set; }
    public TenantId TenantId { get; private set; } = null!;
    public CampaignId CampaignId { get; private set; } = null!;
    public MetricType Metric { get; private set; }
    public long Count { get; private set; }
    public DateTime Date { get; private set; }

    private CampaignMetric() { }

    public CampaignMetric(
        TenantId tenantId,
        CampaignId campaignId,
        MetricType metric,
        long count,
        DateTime date)
    {
        TenantId = tenantId ?? throw new ArgumentNullException(nameof(tenantId));
        CampaignId = campaignId ?? throw new ArgumentNullException(nameof(campaignId));
        Metric = metric;
        Count = count;
        Date = date;
    }

    public void Increment(long value = 1) => Count += value;
}

public enum MetricType
{
    Clicks,
    Impressions,
    ClickToBasket
}
