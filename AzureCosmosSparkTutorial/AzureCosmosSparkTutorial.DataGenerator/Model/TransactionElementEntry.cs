using System;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace AzureCosmosSparkTutorial.Model
{
    public class TransactionElementEntry : IPartitionKeyModel
    {
        [JsonProperty(PropertyName = "id")]
        public Guid Id { get; set; } = Guid.NewGuid();
        public string InvoiceNo { get; set; }
        public string StockCode { get; set; }
        public string Description { get; set; }
        public int Quantity { get; set; }
        public DateTime InvoiceDate { get; set; }
        public double UnitPrice { get; set; }
        public int CustomerId { get; set; }
        public string Country { get; set; }

        public PartitionKey GetPartitionKey() => new(Country);
    }
}