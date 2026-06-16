using System.Security.Claims;
using System.Text.Json;
using RetailMedia.Application.Interfaces;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Api.Middleware;

public class TenantContextMiddleware
{
    private readonly RequestDelegate _next;

    public TenantContextMiddleware(RequestDelegate next) => _next = next;

    public async Task InvokeAsync(HttpContext context, ITenantContext tenantContext)
    {
        // Skip tenant check for health probes and non-API paths
        if (context.Request.Path.StartsWithSegments("/healthz"))
        {
            await _next(context);
            return;
        }

        var tenantId = ExtractTenantId(context);
        if (tenantId == null)
        {
            context.Response.StatusCode = 401;
            context.Response.ContentType = "application/json";
            await context.Response.WriteAsync(JsonSerializer.Serialize(new { error = "TenantId is required" }));
            return;
        }

        if (tenantContext is TenantContext tc)
            tc.SetTenantId(TenantId.From(tenantId));

        await _next(context);
    }

    private static string? ExtractTenantId(HttpContext context)
    {
        // 1. Check JWT claims
        var claim = context.User?.FindFirst("tenantId")?.Value;
        if (!string.IsNullOrWhiteSpace(claim)) return claim;

        // 2. Check X-Tenant-Id header
        var header = context.Request.Headers["X-Tenant-Id"].FirstOrDefault();
        if (!string.IsNullOrWhiteSpace(header)) return header;

        return null;
    }
}
