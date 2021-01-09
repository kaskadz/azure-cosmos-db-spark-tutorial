using Microsoft.Azure.Cosmos;

namespace AzureCosmosSparkTutorial.DataGenerator.Model
{
    public interface IPartitionKeyModel
    {
        public PartitionKey GetPartitionKey();
    }
}