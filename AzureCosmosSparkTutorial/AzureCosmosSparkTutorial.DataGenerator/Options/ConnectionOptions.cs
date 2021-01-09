using System.ComponentModel.DataAnnotations;

namespace AzureCosmosSparkTutorial.DataGenerator.Options
{
    public class ConnectionOptions
    {
        public const string Section = "Connection";

        [Required] public string EndpointUri { get; set; }
        [Required] public string PrimaryKey { get; set; }
    }
}