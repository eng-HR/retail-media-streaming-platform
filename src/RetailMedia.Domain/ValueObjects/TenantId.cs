namespace RetailMedia.Domain.ValueObjects;

public record TenantId(string Value)
{
    public static TenantId From(string value) =>
        string.IsNullOrWhiteSpace(value)
            ? throw new ArgumentException("TenantId cannot be empty", nameof(value))
            : new TenantId(value);

    public override string ToString() => Value;
}
