namespace RetailMedia.Domain.ValueObjects;

public record CampaignId(string Value)
{
    public static CampaignId From(string value) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new ArgumentException("CampaignId cannot be empty", nameof(value))
            : new CampaignId(value);

    public override string ToString() => Value;
}
