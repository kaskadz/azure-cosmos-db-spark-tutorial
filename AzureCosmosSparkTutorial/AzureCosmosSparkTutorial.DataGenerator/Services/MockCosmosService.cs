using System.IO;
using System.Threading.Tasks;
using AzureCosmosSparkTutorial.DataGenerator.Model;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace AzureCosmosSparkTutorial.DataGenerator.Services
{
    public class MockCosmosService : ICosmosService
    {
        public Task<Database> CreateDatabaseAsync(string databaseId)
        {
            return Task.FromResult<Database>(null);
        }

        public Task<Container> CreateContainerAsync(Database database, string containerId, string partitionKeyPath)
        {
            return Task.FromResult<Container>(null);
        }

        public async Task AddItemToContainerAsync<T>(Container container, T item) where T : IPartitionKeyModel
        {
            string line = JsonConvert.SerializeObject(item);
            await File.AppendAllLinesAsync("out.ndjson", new[] {line});
        }

        public Task DeleteDatabase(Database database)
        {
            return Task.CompletedTask;
        }

        public void Dispose()
        {
        }
    }
}