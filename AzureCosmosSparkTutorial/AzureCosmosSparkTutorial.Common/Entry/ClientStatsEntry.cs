using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace AzureCosmosSparkTutorial.Common.Entry
{
    public class ClientStatsEntry : IPartitionKeyModel
    {
        public static readonly string PartitionKeyPath = $"/{nameof(ClientID)}";
        
        [JsonProperty(PropertyName = "id")] public string ClientID { get; set; }
        public double ClientMoneySpent { get; set; }

        public PartitionKey GetPartitionKey() => new PartitionKey(ClientID);
    }
}