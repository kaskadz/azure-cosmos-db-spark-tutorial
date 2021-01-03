using Microsoft.Azure.Cosmos;

namespace AzureCosmosSparkTutorial.Model
{
    public interface IPartitionKeyModel
    {
        public PartitionKey GetPartitionKey();
    }
}