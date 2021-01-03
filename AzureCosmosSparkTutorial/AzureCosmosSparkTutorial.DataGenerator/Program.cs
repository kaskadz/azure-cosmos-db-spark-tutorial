using System;
using System.Threading.Tasks;
using AzureCosmosSparkTutorial.DataGenerator.Options;
using AzureCosmosSparkTutorial.DataGenerator.Services;
using AzureCosmosSparkTutorial.Model;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

namespace AzureCosmosSparkTutorial.DataGenerator
{
    internal class Program
    {
        public static async Task Main(string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddCommandLine(args)
                .Build();

            var connectionOptions = configuration
                .GetSection(ConnectionOptions.Section)
                .Get<ConnectionOptions>() ?? throw new InvalidOperationException("Couldn't read connection options");

            string sourceFilePath = configuration.GetValue<string>("file") ??
                                    throw new InvalidOperationException("Source file was not provided");

            int skip = configuration.GetValue<int>("skip");
            int count = configuration.GetValue<int>("count");

            var cosmosClient = new CosmosClient(connectionOptions.EndpointUri, connectionOptions.PrimaryKey);
            var cosmosService = new CosmosService(cosmosClient);
            var ingestDataReader = new IngestDataReader(sourceFilePath);
            var dataSupplier = new DataSupplier(cosmosService, ingestDataReader);

            try
            {
                Console.WriteLine("Beginning operations...\n");
                await Execute(cosmosService, dataSupplier, skip, count);
            }
            catch (CosmosException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}", de.StatusCode, de);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
            }
            finally
            {
                cosmosClient.Dispose();
                cosmosService.Dispose();
                Console.WriteLine("Completed.");
            }
        }

        private static async Task Execute(CosmosService cosmosService, DataSupplier dataSupplier, int skip, int count)
        {
            var retailDb = await cosmosService.CreateDatabaseAsync("RetailDb");
            var transactionsContainer = await cosmosService.CreateContainerAsync(retailDb, "transactions",
                $"/{nameof(TransactionElementEntry.Country)}");

            await dataSupplier.IngestData(transactionsContainer, skip, count);
        }
    }
}