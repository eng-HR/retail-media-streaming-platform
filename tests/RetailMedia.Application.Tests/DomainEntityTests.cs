using RetailMedia.Domain.Entities;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Application.Tests;

public class DomainEntityTests
{
    private static readonly TenantId TenantId = TenantId.From("tesco");
    private static readonly CampaignId CampaignId = CampaignId.From("cmp_789");

    [Fact]
    public void Campaign_Create_SetsProperties()
    {
        var campaign = new Campaign(CampaignId, TenantId, "Summer Sale 2026");

        Assert.Equal(CampaignId, campaign.Id);
        Assert.Equal(TenantId, campaign.TenantId);
        Assert.Equal("Summer Sale 2026", campaign.Name);
        Assert.True(campaign.IsActive);
    }

    [Fact]
    public void Campaign_Create_NullId_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new Campaign(null!, TenantId, "test"));
    }

    [Fact]
    public void Campaign_Create_NullTenant_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new Campaign(CampaignId, null!, "test"));
    }

    [Fact]
    public void Campaign_Deactivate_SetsIsActiveFalse()
    {
        var campaign = new Campaign(CampaignId, TenantId, "Test");

        campaign.Deactivate();

        Assert.False(campaign.IsActive);
    }

    [Fact]
    public void Campaign_Rename_UpdatesName()
    {
        var campaign = new Campaign(CampaignId, TenantId, "Old Name");

        campaign.Rename("New Name");

        Assert.Equal("New Name", campaign.Name);
    }

    [Fact]
    public void Campaign_Rename_Null_Throws()
    {
        var campaign = new Campaign(CampaignId, TenantId, "Test");

        Assert.Throws<ArgumentNullException>(() => campaign.Rename(null!));
    }

    [Fact]
    public void CampaignMetric_Create_SetsProperties()
    {
        var date = new DateTime(2026, 1, 15);
        var metric = new CampaignMetric(TenantId, CampaignId, MetricType.Clicks, 100, date);

        Assert.Equal(TenantId, metric.TenantId);
        Assert.Equal(CampaignId, metric.CampaignId);
        Assert.Equal(MetricType.Clicks, metric.Metric);
        Assert.Equal(100, metric.Count);
        Assert.Equal(date, metric.Date);
    }

    [Fact]
    public void CampaignMetric_Increment_AddsToCount()
    {
        var metric = new CampaignMetric(TenantId, CampaignId, MetricType.Impressions, 50, DateTime.UtcNow);

        metric.Increment(10);

        Assert.Equal(60, metric.Count);
    }

    [Fact]
    public void CampaignMetric_Increment_DefaultAddsOne()
    {
        var metric = new CampaignMetric(TenantId, CampaignId, MetricType.ClickToBasket, 5, DateTime.UtcNow);

        metric.Increment();

        Assert.Equal(6, metric.Count);
    }

    [Fact]
    public void Event_Create_SetsProperties()
    {
        var timestamp = DateTime.UtcNow;
        var metadata = new Dictionary<string, string> { ["key"] = "value" };
        var evt = new Event("evt_001", TenantId, "user_1", CampaignId, EventType.AdClick, timestamp, metadata);

        Assert.Equal("evt_001", evt.EventId);
        Assert.Equal(TenantId, evt.TenantId);
        Assert.Equal("user_1", evt.UserId);
        Assert.Equal(CampaignId, evt.CampaignId);
        Assert.Equal(EventType.AdClick, evt.Type);
        Assert.Equal(timestamp, evt.Timestamp);
        Assert.Equal(metadata, evt.Metadata);
    }

    [Fact]
    public void Event_Create_NullEventId_Throws()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new Event(null!, TenantId, "user_1", CampaignId, EventType.AdImpression, DateTime.UtcNow, null));
    }

    [Fact]
    public void Event_Create_WithMetadata_StoresMetadata()
    {
        var metadata = new Dictionary<string, string>
        {
            ["productId"] = "prod_123",
            ["price"] = "29.99"
        };
        var evt = new Event("evt_002", TenantId, "user_2", CampaignId, EventType.AddToCart, DateTime.UtcNow, metadata);

        Assert.Equal(2, evt.Metadata!.Count);
        Assert.Equal("prod_123", evt.Metadata["productId"]);
    }

    [Fact]
    public void TenantId_FromValid_CreatesInstance()
    {
        var tenantId = TenantId.From("valid-tenant");

        Assert.Equal("valid-tenant", tenantId.Value);
    }

    [Fact]
    public void TenantId_FromEmpty_Throws()
    {
        Assert.Throws<ArgumentException>(() => TenantId.From(""));
    }

    [Fact]
    public void TenantId_FromWhitespace_Throws()
    {
        Assert.Throws<ArgumentException>(() => TenantId.From("   "));
    }

    [Fact]
    public void CampaignId_FromValid_CreatesInstance()
    {
        var campaignId = CampaignId.From("valid-campaign");

        Assert.Equal("valid-campaign", campaignId.Value);
    }

    [Fact]
    public void CampaignId_FromEmpty_Throws()
    {
        Assert.Throws<ArgumentException>(() => CampaignId.From(""));
    }

    [Fact]
    public void TenantId_Equality_SameValue_AreEqual()
    {
        var id1 = TenantId.From("tesco");
        var id2 = TenantId.From("tesco");

        Assert.Equal(id1, id2);
    }

    [Fact]
    public void TenantId_Equality_DifferentValue_AreNotEqual()
    {
        var id1 = TenantId.From("tesco");
        var id2 = TenantId.From("walmart");

        Assert.NotEqual(id1, id2);
    }

    [Fact]
    public void CampaignId_ToString_ReturnsValue()
    {
        var id = CampaignId.From("cmp_789");

        Assert.Equal("cmp_789", id.ToString());
    }

    [Fact]
    public void EventType_Values_Exist()
    {
        Assert.Equal(5, Enum.GetValues<EventType>().Length);
        Assert.Contains(EventType.ProductView, Enum.GetValues<EventType>());
        Assert.Contains(EventType.AddToCart, Enum.GetValues<EventType>());
        Assert.Contains(EventType.Purchase, Enum.GetValues<EventType>());
        Assert.Contains(EventType.AdImpression, Enum.GetValues<EventType>());
        Assert.Contains(EventType.AdClick, Enum.GetValues<EventType>());
    }
}
