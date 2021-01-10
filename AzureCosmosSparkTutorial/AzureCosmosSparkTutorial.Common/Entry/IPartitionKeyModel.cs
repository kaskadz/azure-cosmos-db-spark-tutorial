using Microsoft.Azure.Cosmos;

namespace AzureCosmosSparkTutorial.Common.Entry
{
    public interface IPartitionKeyModel
    {
        public PartitionKey GetPartitionKey();
    }
}