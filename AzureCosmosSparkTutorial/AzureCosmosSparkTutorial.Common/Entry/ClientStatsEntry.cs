using Microsoft.Azure.Cosmos;

namespace AzureCosmosSparkTutorial.Common.Entry
{
    public class ClientStatsEntry : EntryBase, IPartitionKeyModel
    {
        public string ClientID { get; set; }
        public double ClientMoneySpent { get; set; }

        public PartitionKey GetPartitionKey() => new PartitionKey(ClientID);
    }
}