using System.Text.Json;
using RetailMedia.Domain.Interfaces;
using StackExchange.Redis;

namespace RetailMedia.Infrastructure.Caching;

public class RedisCache : IRedisCache, IDisposable
{
    private readonly ConnectionMultiplexer _redis;
    private readonly IDatabase _db;

    public RedisCache(string connectionString)
    {
        _redis = ConnectionMultiplexer.Connect(connectionString);
        _db = _redis.GetDatabase();
    }

    public async Task<long> IncrementCounterAsync(string key, long value = 1) =>
        await _db.StringIncrementAsync(key, value);

    public async Task<long> GetCounterAsync(string key)
    {
        var val = await _db.StringGetAsync(key);
        return val.HasValue ? (long)val : 0;
    }

    public async Task SetAsync<T>(string key, T value, TimeSpan? expiry = null)
    {
        var json = JsonSerializer.Serialize(value);
        if (expiry.HasValue)
            await _db.StringSetAsync(key, json, expiry.Value, When.Always);
        else
            await _db.StringSetAsync(key, json);
    }

    public async Task<T?> GetAsync<T>(string key)
    {
        var json = await _db.StringGetAsync(key);
        return json.HasValue ? JsonSerializer.Deserialize<T>((string)json!) : default;
    }

    public async Task<bool> KeyExistsAsync(string key) =>
        await _db.KeyExistsAsync(key);

    public async Task<long> GetAndResetCounterAsync(string key)
    {
        var val = await _db.StringGetSetAsync(key, 0);
        return val.HasValue ? (long)val : 0;
    }

    public void Dispose() => _redis?.Dispose();
}
