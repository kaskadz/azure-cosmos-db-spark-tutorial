using System;
using Newtonsoft.Json;

namespace AzureCosmosSparkTutorial.Common.Entry
{
    public abstract class EntryBase
    {
        [JsonProperty(PropertyName = "id")] public Guid Id { get; set; } = Guid.NewGuid();
    }
}