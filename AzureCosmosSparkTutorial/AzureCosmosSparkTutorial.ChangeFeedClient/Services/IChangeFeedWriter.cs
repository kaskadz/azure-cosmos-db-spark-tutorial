using System;
using System.Threading.Tasks;

namespace AzureCosmosSparkTutorial.ChangeFeedClient.Services
{
    public interface IChangeFeedWriter : IDisposable
    {
        Task ReportChangesAsync();
    }
}