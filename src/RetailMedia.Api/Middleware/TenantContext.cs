using RetailMedia.Application.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Api.Middleware;

public class TenantContext : ITenantContext
{
    public TenantId CurrentTenantId { get; private set; } = null!;

    public void SetTenantId(TenantId tenantId) => CurrentTenantId = tenantId;
}
