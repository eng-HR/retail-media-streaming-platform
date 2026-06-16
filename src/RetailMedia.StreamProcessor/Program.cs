using RetailMedia.Application;
using RetailMedia.Infrastructure;
using RetailMedia.StreamProcessor;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddApplication();
builder.Services.AddInfrastructure(builder.Configuration);

builder.Services.AddSingleton(sp =>
{
    var config = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
    var logger = sp.GetRequiredService<ILogger<KafkaEventConsumer>>();
    return new KafkaEventConsumer(config, sp, logger);
});

builder.Services.AddHostedService(sp => sp.GetRequiredService<KafkaEventConsumer>());

var host = builder.Build();
host.Run();
