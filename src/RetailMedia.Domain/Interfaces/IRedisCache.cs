namespace RetailMedia.Domain.Interfaces;

public interface IRedisCache
{
    Task<long> IncrementCounterAsync(string key, long value = 1, TimeSpan? expiry = null);
    Task<long> GetCounterAsync(string key);
    Task SetAsync<T>(string key, T value, TimeSpan? expiry = null);
    Task<T?> GetAsync<T>(string key);
    Task<bool> KeyExistsAsync(string key);
    Task<long> GetAndResetCounterAsync(string key);
}
