using System;
using System.Collections.Generic;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace AzureCosmosSparkTutorial.Common.Entry
{
    public class TransactionElementEntry : IPartitionKeyModel
    {
        public static readonly string PartitionKeyPath = $"/{nameof(Country)}";

        [JsonProperty(PropertyName = "id")] public Guid Id { get; set; } = Guid.NewGuid();
        public DateTime InvoiceDate { get; set; }
        public string Country { get; set; }
        public long? CustomerId { get; set; }

        public IEnumerable<ItemEntry> Items { get; set; }

        public PartitionKey GetPartitionKey() => new(Country);
    }
}