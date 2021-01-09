using System.ComponentModel.DataAnnotations;

namespace AzureCosmosSparkTutorial.DataGenerator.Options
{
    public class CommandLineOptions
    {
        [Required] public string File { get; set; }
        [Required] public int Skip { get; set; }
        [Required] public int Take { get; set; }
    }
}