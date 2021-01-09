using System;
using System.Threading.Tasks;
using AzureCosmosSparkTutorial.DataGenerator.Model;
using AzureCosmosSparkTutorial.DataGenerator.Options;
using AzureCosmosSparkTutorial.DataGenerator.Services;
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

            var commandLineOptions = configuration.Get<CommandLineOptions>();

            ICosmosService cosmosService = new MockCosmosService();
            var ingestDataReader = new IngestDataReader(commandLineOptions.File);
            var dataSupplier = new DataSupplier(cosmosService, ingestDataReader);

            try
            {
                Console.WriteLine("Beginning operations...\n");
                await Execute(cosmosService, dataSupplier, commandLineOptions.Skip, commandLineOptions.Take);
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
                cosmosService.Dispose();
                Console.WriteLine("Completed.");
            }
        }

        private static ICosmosService CreateCosmosService(ConnectionOptions options)
        {
            var cosmosClient = new CosmosClient(options.EndpointUri, options.PrimaryKey);
            return new CosmosService(cosmosClient);
        }

        private static async Task Execute(ICosmosService cosmosService, DataSupplier dataSupplier, int skip, int count)
        {
            var retailDb = await cosmosService.CreateDatabaseAsync("RetailDb");
            var transactionsContainer = await cosmosService.CreateContainerAsync(retailDb, "invoices",
                $"/{nameof(TransactionElementEntry.Country)}");

            await dataSupplier.IngestData(transactionsContainer, skip, count);
        }
    }
}