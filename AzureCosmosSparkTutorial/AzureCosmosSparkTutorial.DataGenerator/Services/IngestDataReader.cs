using System;
using System.Collections.Generic;
using System.IO;
using AzureCosmosSparkTutorial.Common.Entry;
using Newtonsoft.Json;

namespace AzureCosmosSparkTutorial.DataGenerator.Services
{
    public class IngestDataReader
    {
        private readonly string _sourceFilePath;

        public IngestDataReader(string sourceFilePath)
        {
            _sourceFilePath = sourceFilePath;
        }

        public async IAsyncEnumerable<TransactionElementEntry> GetTransactionsAsyncEnumerable()
        {
            using var file = File.OpenText(_sourceFilePath);
            while (!file.EndOfStream)
            {
                var readLineAsync = await file.ReadLineAsync();
                if (readLineAsync == null)
                {
                    continue;
                }

                yield return DeserializeEntry(readLineAsync);
            }
        }

        private static TransactionElementEntry DeserializeEntry(string serializedEntry)
        {
            var jsonSerializerSettings = new JsonSerializerSettings();
            var deserializeObject =
                JsonConvert.DeserializeObject<TransactionElementEntry>(serializedEntry, jsonSerializerSettings);
            deserializeObject.InvoiceDate = DateTime.Now;
            return deserializeObject;
        }
    }
}