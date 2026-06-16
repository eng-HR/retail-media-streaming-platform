using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Application.Interfaces;

public interface ITenantContext
{
    TenantId CurrentTenantId { get; }
}
