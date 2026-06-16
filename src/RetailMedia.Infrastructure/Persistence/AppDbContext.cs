using Microsoft.EntityFrameworkCore;
using RetailMedia.Domain.Entities;
using RetailMedia.Domain.ValueObjects;

namespace RetailMedia.Infrastructure.Persistence;

public class AppDbContext : DbContext
{
    public DbSet<Campaign> Campaigns => Set<Campaign>();
    public DbSet<Event> Events => Set<Event>();
    public DbSet<CampaignMetric> CampaignMetrics => Set<CampaignMetric>();

    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Campaign>(e =>
        {
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).HasConversion(
                id => id.Value, value => CampaignId.From(value));
            e.Property(x => x.TenantId).HasConversion(
                id => id.Value, value => TenantId.From(value));
            e.Property(x => x.Name).HasMaxLength(256);
            e.HasIndex(x => new { x.TenantId, x.Id });
        });

        modelBuilder.Entity<Event>(e =>
        {
            e.HasKey(x => x.EventId);
            e.Property(x => x.TenantId).HasConversion(
                id => id.Value, value => TenantId.From(value));
            e.Property(x => x.CampaignId).HasConversion(
                id => id.Value, value => CampaignId.From(value));
            e.Property(x => x.EventId).HasMaxLength(128);
            e.Property(x => x.UserId).HasMaxLength(128);
            e.Property(x => x.Type).HasConversion<string>().HasMaxLength(32);
            e.Property(x => x.Metadata).HasColumnType("jsonb");
            e.HasIndex(x => new { x.TenantId, x.CampaignId, x.Timestamp });
        });

        modelBuilder.Entity<CampaignMetric>(e =>
        {
            e.HasKey(x => x.Id);
            e.Property(x => x.Id).ValueGeneratedOnAdd();
            e.Property(x => x.TenantId).HasConversion(
                id => id.Value, value => TenantId.From(value));
            e.Property(x => x.CampaignId).HasConversion(
                id => id.Value, value => CampaignId.From(value));
            e.Property(x => x.Metric).HasConversion<string>().HasMaxLength(32);
            e.HasIndex(x => new { x.TenantId, x.CampaignId, x.Metric, x.Date });
        });
    }
}
