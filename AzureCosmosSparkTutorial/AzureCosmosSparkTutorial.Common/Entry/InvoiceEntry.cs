using System;
using System.Collections.Generic;
using Microsoft.Azure.Cosmos;

namespace AzureCosmosSparkTutorial.Common.Entry
{
    public class TransactionElementEntry : EntryBase, IPartitionKeyModel
    {
        public static readonly string PartitionKeyPath = $"/{nameof(Country)}";

        public DateTime InvoiceDate { get; set; }
        public string Country { get; set; }
        public long? CustomerId { get; set; }
        
        public IEnumerable<ItemEntry> Items { get; set; }

        public PartitionKey GetPartitionKey() => new(Country);
    }
}