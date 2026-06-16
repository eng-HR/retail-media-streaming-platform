using RetailMedia.Application.DTOs;
using RetailMedia.Application.Interfaces;

namespace RetailMedia.EventCollector.Endpoints;

public static class EventEndpoints
{
    public static void MapEventEndpoints(this WebApplication app)
    {
        app.MapPost("/events", async (IngestEventRequest request,
            IEventIngestionService service, CancellationToken ct) =>
        {
            await service.IngestAsync(request, ct);
            return Results.Accepted($"/events/{request.EventId}", new
            {
                data = new { eventId = request.EventId, status = "accepted" },
                meta = new { timestamp = DateTime.UtcNow }
            });
        })
        .WithName("IngestEvent")
        .WithTags("Events");
    }
}
