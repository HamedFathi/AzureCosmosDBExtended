// ReSharper disable IdentifierTypo
// ReSharper disable UnusedMember.Global
// ReSharper disable InconsistentNaming

using Microsoft.Azure.Cosmos;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace AzureCosmosDBExtended
{
    public static class AzureCosmosDBExtensions
    {
        public static async IAsyncEnumerable<T> AsAsyncEnumerable<T>(this FeedIterator<T> iterator,
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            while (iterator.HasMoreResults)
            {
                var page = await iterator.ReadNextAsync(cancellationToken);
                foreach (var item in page)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return item;
                }
            }
        }

        public static async Task<IEnumerable<string>> BulkCreateAsync<T>(this Container container, IEnumerable<T> data,
            PartitionKey? partitionKey = null)
        {
            var tasks = new List<Task>();
            var errors = new List<string>();
            foreach (var item in data)
            {
                tasks.Add(container.CreateItemAsync(item, partitionKey)
                    .ContinueWith(itemResponse =>
                    {
                        if (!itemResponse.IsCompletedSuccessfully)
                        {
                            var innerExceptions = itemResponse.Exception?.Flatten();
                            if (innerExceptions?.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException)
                                is CosmosException cosmosException)
                            {
                                errors.Add($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                            }
                            else
                            {
                                if (innerExceptions != null)
                                    errors.Add($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                            }
                        }
                    }));
            }

            await Task.WhenAll(tasks);
            return errors;
        }

        public static async Task<IEnumerable<string>> BulkUpsertAsync<T>(this Container container, IEnumerable<T> data,
            PartitionKey? partitionKey = null)
        {
            var tasks = new List<Task>();
            var errors = new List<string>();
            foreach (var item in data)
            {
                tasks.Add(container.UpsertItemAsync(item, partitionKey)
                    .ContinueWith(itemResponse =>
                    {
                        if (!itemResponse.IsCompletedSuccessfully)
                        {
                            var innerExceptions = itemResponse.Exception?.Flatten();
                            if (innerExceptions?.InnerExceptions.FirstOrDefault(innerEx => innerEx is CosmosException)
                                is CosmosException cosmosException)
                            {
                                errors.Add($"Received {cosmosException.StatusCode} ({cosmosException.Message}).");
                            }
                            else
                            {
                                if (innerExceptions != null)
                                    errors.Add($"Exception {innerExceptions.InnerExceptions.FirstOrDefault()}.");
                            }
                        }
                    }));
            }

            await Task.WhenAll(tasks);
            return errors;
        }

        public static async Task<bool> ContainerExistsAsync(this CosmosClient cosmosClient, string databaseName,
            string containerName)
        {
            var databaseExists = await cosmosClient.DatabaseExistsAsync(databaseName);
            if (!databaseExists)
            {
                return false;
            }

            var containerNames = new List<string>();
            var database = cosmosClient.GetDatabase(databaseName);
            using (var iterator = database.GetContainerQueryIterator<ContainerProperties>())
            {
                while (iterator.HasMoreResults)
                {
                    foreach (var containerProperties in await iterator.ReadNextAsync())
                    {
                        containerNames.Add(containerProperties.Id);
                    }
                }
            }

            return containerNames.Contains(containerName);
        }

        public static async Task<bool> DatabaseExistsAsync(this CosmosClient cosmosClient, string databaseName)
        {
            var databaseNames = new List<string>();
            using (var iterator = cosmosClient.GetDatabaseQueryIterator<DatabaseProperties>())
            {
                while (iterator.HasMoreResults)
                {
                    foreach (var databaseProperties in await iterator.ReadNextAsync())
                    {
                        databaseNames.Add(databaseProperties.Id);
                    }
                }
            }

            return databaseNames.Contains(databaseName);
        }
    }
}