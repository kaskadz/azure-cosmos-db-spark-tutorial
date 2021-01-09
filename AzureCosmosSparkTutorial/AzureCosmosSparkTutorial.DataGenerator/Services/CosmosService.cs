using System;
using System.Net;
using System.Threading.Tasks;
using AzureCosmosSparkTutorial.DataGenerator.Model;
using Microsoft.Azure.Cosmos;

namespace AzureCosmosSparkTutorial.DataGenerator.Services
{
    public class CosmosService : ICosmosService
    {
        private readonly CosmosClient _cosmosClient;

        public CosmosService(CosmosClient cosmosClient)
        {
            _cosmosClient = cosmosClient;
        }

        /// <summary>
        /// Create the database if it does not exist
        /// </summary>
        public async Task<Database> CreateDatabaseAsync(string databaseId)
        {
            DatabaseResponse database = await _cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Created Database: {0}\n", database.Database.Id);
            return database;
        }

        /// <summary>
        /// Create the container if it does not exist. 
        /// </summary>
        public async Task<Container> CreateContainerAsync(Database database, string containerId,
            string partitionKeyPath)
        {
            var containerProperties = new ContainerProperties(containerId, partitionKeyPath)
            {
                AnalyticalStoreTimeToLiveInSeconds = -1
            };
            var throughputProperties = ThroughputProperties.CreateManualThroughput(400);
            var containerRequestOptions = new ContainerRequestOptions();
            var container = await database.CreateContainerIfNotExistsAsync(
                containerProperties,
                throughputProperties,
                containerRequestOptions);
            Console.WriteLine("Created Container: {0}\n", container.Container.Id);
            return container;
        }

        public async Task AddItemToContainerAsync<T>(Container container, T item)
            where T : IPartitionKeyModel
        {
            try
            {
                await container.CreateItemAsync(item, item.GetPartitionKey());
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                Console.WriteLine("Item in database with id: {0} already exists\n", item);
            }
        }

        public async Task DeleteDatabase(Database database)
        {
            await database.DeleteAsync();
            Console.WriteLine("Deleted Database: {0}\n", database.Id);
        }

        public void Dispose()
        {
            _cosmosClient?.Dispose();
        }
    }
}