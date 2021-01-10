using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using AzureCosmosSparkTutorial.ChangeFeedClient.Services;
using AzureCosmosSparkTutorial.Common.Entry;
using AzureCosmosSparkTutorial.Common.Utilities;
using Newtonsoft.Json;

namespace AzureCosmosSparkTutorial.ChangeFeedClient
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var (commandLineOptions, connectionOptions) = ConfigurationLoader.LoadConfiguration(args);

            var subscription = commandLineOptions.Sub;
            Task task = subscription switch
            {
                "clients" => ChangeFeedConsoleWriter<ClientStatsEntry>.InitializeAsync(connectionOptions, "RetailDb",
                    "clients", "/ClientID", ChangeHandler),
                "countries" => ChangeFeedConsoleWriter<CountryStatsEntry>.InitializeAsync(connectionOptions, "RetailDb",
                    "countries", "/Country", ChangeHandler),
                _ => WriteHelp()
            };

            await task;
        }

        private static Task WriteHelp()
        {
            Console.WriteLine("You have to specify --sub parameter { clients, countries }");
            return Task.CompletedTask;
        }

        private static Task ChangeHandler<T>(IReadOnlyCollection<T> changes,
            CancellationToken cancellationtoken)
        {
            Console.WriteLine(string.Join("", Enumerable.Repeat("-", 3)));
            foreach (var entry in changes)
            {
                Console.WriteLine(JsonConvert.SerializeObject(entry, Formatting.None));
            }

            return Task.CompletedTask;
        }
    }
}