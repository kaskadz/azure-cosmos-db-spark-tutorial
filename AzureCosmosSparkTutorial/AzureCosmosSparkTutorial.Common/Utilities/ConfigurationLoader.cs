using System;
using AzureCosmosSparkTutorial.Common.Options;
using Microsoft.Extensions.Configuration;

namespace AzureCosmosSparkTutorial.Common.Utilities
{
    public static class ConfigurationLoader
    {
        public static (CommandLineOptions commandLineOptions, ConnectionOptions connectionOptions) LoadConfiguration(
            params string[] args)
        {
            IConfiguration configuration = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
                .AddCommandLine(args)
                .Build();

            var connectionOptions = configuration
                .GetSection(ConnectionOptions.Section)
                .Get<ConnectionOptions>() ?? throw new InvalidOperationException("Couldn't read connection options");

            var commandLineOptions = configuration.Get<CommandLineOptions>();

            return (commandLineOptions, connectionOptions);
        }
    }
}