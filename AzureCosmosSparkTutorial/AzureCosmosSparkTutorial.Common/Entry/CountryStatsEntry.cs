using Microsoft.Azure.Cosmos;

namespace AzureCosmosSparkTutorial.Common.Entry
{
    public class CountryStatsEntry : EntryBase, IPartitionKeyModel
    {
        public string Country { get; set; }
        public long Invoices { get; set; }
        
        public PartitionKey GetPartitionKey() => new PartitionKey(Country);
    }
}