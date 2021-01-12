using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace AzureCosmosSparkTutorial.Common.Entry
{
    public class CountryStatsEntry : IPartitionKeyModel
    {
        public static readonly string PartitionKeyPath = $"/{nameof(Country)}";
        
        [JsonProperty(PropertyName = "id")] public string Country { get; set; }
        public long Invoices { get; set; }
        
        public PartitionKey GetPartitionKey() => new PartitionKey(Country);
    }
}