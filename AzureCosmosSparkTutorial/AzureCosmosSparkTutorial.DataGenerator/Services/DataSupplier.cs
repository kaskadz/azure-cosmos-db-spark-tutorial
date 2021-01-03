using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using AzureCosmosSparkTutorial.Model;
using Microsoft.Azure.Cosmos;
using ShellProgressBar;

namespace AzureCosmosSparkTutorial.DataGenerator.Services
{
    public class DataSupplier
    {
        private readonly CosmosService _cosmosService;
        private readonly IngestDataReader _ingestDataReader;

        private static readonly ProgressBarOptions ProgressBarOptions = new()
        {
            ProgressCharacter = '▮',
            BackgroundCharacter = '—',
            ForegroundColor = ConsoleColor.Yellow,
            BackgroundColor = ConsoleColor.Gray,
            ForegroundColorDone = ConsoleColor.Green,
            ProgressBarOnBottom = false,
            ShowEstimatedDuration = true,
            DisplayTimeInRealTime = true
        };

        public DataSupplier(CosmosService cosmosService, IngestDataReader ingestDataReader)
        {
            _cosmosService = cosmosService;
            _ingestDataReader = ingestDataReader;
        }

        public async Task IngestData(Container container, int skip, int count)
        {
            IAsyncEnumerable<TransactionElementEntry> transactionsAsyncEnumerable = _ingestDataReader
                .GetTransactionsAsyncEnumerable()
                .Skip(skip)
                .Take(count);

            Console.OutputEncoding = System.Text.Encoding.UTF8;
            using var progress = new ProgressBar(
                maxTicks: count,
                message: $"Documents progress",
                options: ProgressBarOptions);

            var stopwatch = Stopwatch.StartNew();
            await foreach (var transactionElementEntry in transactionsAsyncEnumerable)
            {
                await _cosmosService.AddItemToContainerAsync(container, transactionElementEntry);
                progress.Tick();
                progress.EstimatedDuration = CalculateEstimatedTimespan(count, stopwatch, progress);
            }
            stopwatch.Stop();
        }

        private static TimeSpan CalculateEstimatedTimespan(int totalCount, Stopwatch stopwatch, ProgressBar progress)
        {
            double estimatedSeconds = stopwatch.Elapsed.TotalSeconds / progress.CurrentTick *
                                      (totalCount - progress.CurrentTick);
            var estimatedTimespan = double.IsInfinity(estimatedSeconds)
                ? TimeSpan.MaxValue
                : TimeSpan.FromSeconds(estimatedSeconds);
            return estimatedTimespan;
        }
    }
}