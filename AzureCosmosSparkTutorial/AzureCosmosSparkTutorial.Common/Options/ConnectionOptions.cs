namespace AzureCosmosSparkTutorial.Common.Options
{
    public class ConnectionOptions
    {
        public const string Section = "Connection";

        public string EndpointUri { get; set; }
        public string PrimaryKey { get; set; }
    }
}