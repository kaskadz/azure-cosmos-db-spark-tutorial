using System;
using System.Threading.Tasks;
using AzureCosmosSparkTutorial.Common.Entry;
using Microsoft.Azure.Cosmos;

namespace AzureCosmosSparkTutorial.DataGenerator.Services
{
    public interface ICosmosService : IDisposable
    {
        /// <summary>
        /// Create the database if it does not exist
        /// </summary>
        Task<Database> CreateDatabaseAsync(string databaseId);

        /// <summary>
        /// Create the container if it does not exist. 
        /// </summary>
        Task<Container> CreateContainerAsync(Database database, string containerId, string partitionKeyPath);

        Task AddItemToContainerAsync<T>(Container container, T item) where T : IPartitionKeyModel;
    }
}