using System;
using System.Threading.Tasks;
using AzureCosmosSparkTutorial.Common.Options;
using Microsoft.Azure.Cosmos;

namespace AzureCosmosSparkTutorial.ChangeFeedClient.Services
{
    public class ChangeFeedConsoleWriter<T> : IChangeFeedWriter
    {
        private readonly CosmosClient _cosmosClient;
        private readonly ChangeFeedProcessor _changeFeedProcessor;

        public static async Task<IChangeFeedWriter> InitializeAsync(ConnectionOptions connectionOptions,
            string databaseName,
            string containerName,
            string partitionKeyPath,
            Container.ChangesHandler<T> changesHandler)
        {
            var cosmosClient = new CosmosClient(connectionOptions.EndpointUri, connectionOptions.PrimaryKey);

            Database database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseName);

            Container container =
                await database.CreateContainerIfNotExistsAsync(containerName, partitionKeyPath);

            Container leaseContainer =
                await database.CreateContainerIfNotExistsAsync($"{containerName}Lease", partitionKeyPath);

            ChangeFeedProcessor changeFeedProcessor = container
                .GetChangeFeedProcessorBuilder($"{containerName}ChangeFeed", changesHandler)
                .WithInstanceName("consoleProcessor")
                .WithLeaseContainer(leaseContainer)
                .Build();

            return new ChangeFeedConsoleWriter<T>(cosmosClient, changeFeedProcessor);
        }

        private ChangeFeedConsoleWriter(CosmosClient cosmosClient, ChangeFeedProcessor changeFeedProcessor)
        {
            _cosmosClient = cosmosClient;
            _changeFeedProcessor = changeFeedProcessor;
        }

        public async Task ReportChangesAsync()
        {
            await _changeFeedProcessor.StartAsync();

            while (Console.ReadKey().Key != ConsoleKey.Q)
            {
            }

            await _changeFeedProcessor.StopAsync();
        }

        public void Dispose()
        {
            try
            {
                _changeFeedProcessor.StopAsync().GetAwaiter().GetResult();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }

            _cosmosClient.Dispose();
        }
    }
}