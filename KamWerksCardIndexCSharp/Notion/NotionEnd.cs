using Notion.Client;
using KamWerksCardIndexCSharp.Helpers;
using System.Collections.Concurrent;
using System.Net;

namespace KamWerksCardIndexCSharp.Notion
{
    internal class NotionEnd
    {
        public static List<string> CtiCardNames { get; private set; } = new();
        public static List<string> CtiSigilNames { get; private set; } = new();
        public static ConcurrentDictionary<string, string> CtiCards { get; private set; } = new();
        public static ConcurrentDictionary<string, string> CtiSigils { get; private set; } = new();
        public static List<string> DmcCardNames { get; private set; } = new();
        public static List<string> DmcSigilNames { get; private set; } = new();
        public static ConcurrentDictionary<string, string> DmcCards { get; private set; } = new();
        public static ConcurrentDictionary<string, string> DmcSigils { get; private set; } = new();
        public static List<string> IotfdCardNames { get; private set; } = new();
        public static List<string> IotfdSigilNames { get; private set; } = new();
        public static ConcurrentDictionary<string, string> IotfdCards { get; private set; } = new();
        public static ConcurrentDictionary<string, string> IotfdSigils { get; private set; } = new();

        public static async Task NotionMain()
        {
            var logger = LoggerFactory.CreateLogger("console");
            string NotionAPIKey = Environment.GetEnvironmentVariable("NOTION_API_KEY");
            if (string.IsNullOrWhiteSpace(NotionAPIKey))
            {
                logger.Error("Hey, You missed the Notion API Key Environment Var.");
                return;
            }

            logger.Info("Connecting to Notion API...");
            var client = NotionClientFactory.Create(new ClientOptions { AuthToken = NotionAPIKey });

            var CtiCardPagesList = new List<string>();
	        CtiCardPagesList = await FetchAllPageIds(client, "01c80bdf-b487-4e6f-b576-4ae546cda356", "CTI Cards");
            CtiCards = await FetchPageNamesAndStore(client, CtiCardPagesList, "Card", CtiCardNames, "CTI Cards");

            var CtiSigilPagesList = new List<string>();
            CtiSigilPagesList = await FetchAllPageIds(client, "538ba07a-6203-41e4-b975-a7db2c213a88", "CTI Sigils");
            CtiSigils = await FetchPageNamesAndStore(client, CtiSigilPagesList, "Sigil", CtiSigilNames, "CTI Sigils");

            var DmcCardPagesList = new List<string>();
            DmcCardPagesList = await FetchAllPageIds(client, "0c218282-c9a1-468e-afca-678ca2a95be4", "DMC Cards");
            DmcCards = await FetchPageNamesAndStore(client, DmcCardPagesList, "Card", DmcCardNames, "DMC Cards");

            var DmcSigilPagesList = new List<string>();
            DmcSigilPagesList = await FetchAllPageIds(client, "a1e99307-5540-451f-843e-cde9ff5581ee", "DMC Sigils");
            DmcSigils = await FetchPageNamesAndStore(client, DmcSigilPagesList, "Sigil", DmcSigilNames, "DMC Sigils");
            
            var IotfdCardPagesList = new List<string>();
            IotfdCardPagesList = await FetchAllPageIds(client, "27c58b74-9db9-4e1d-9dbc-f2ac3bbac469", "IOTFD Cards");
            IotfdCards = await FetchPageNamesAndStore(client, IotfdCardPagesList, "Card", IotfdCardNames, "IOTFD Cards");

            var IotfdSigilPagesList = new List<string>();
            IotfdSigilPagesList = await FetchAllPageIds(client, "9c6b2c64-408b-4f89-8441-82ea17a58427", "IOTFD Sigils");
            IotfdSigils = await FetchPageNamesAndStore(client, IotfdSigilPagesList, "Sigil", IotfdSigilNames, "IOTFD Sigils");

            logger.Info("Notion data retrieval completed successfully.");
        }

        private static async Task<List<string>> FetchAllPageIds(NotionClient client, string dataSourceId, string Group)
        {
	        List<string> pageIds = new();
	        string? nextCursor = null;

	        do
	        {
		        var request = new QueryDataSourceRequest
		        {
			        DataSourceId = dataSourceId,
			        StartCursor = nextCursor,
			        PageSize = 100
		        };

		        var response = await RetryWithBackoff(() => client.DataSources.QueryAsync(request));

		        Console.WriteLine($"[{Group}]: Received {response.Results.Count} results from Data Source {dataSourceId}");
                
		        pageIds.AddRange(response.Results.Select(page => page.Id));
                
		        nextCursor = response.HasMore ? response.NextCursor : null;
	        } while (!string.IsNullOrEmpty(nextCursor));

	        return pageIds;
        }

        public static async Task<T> RetryWithBackoff<T>(Func<Task<T>> action)
        {
            int retryCount = 0;
            int delay = 1000;

            while (true)
            {
                try
                {
                    return await action();
                }
                catch (NotionApiRateLimitException ex) when (ex.StatusCode == HttpStatusCode.TooManyRequests) 
                {
                    int waitTime = ex.RetryAfter?.Milliseconds + 50 ?? delay;

                    Console.WriteLine($"[Retry {retryCount}]: Rate limited! Retrying in {waitTime}ms...");
                    await Task.Delay(waitTime);

                    retryCount++;
                    delay *= 2;
                }
            }
        }

        private static async Task<ConcurrentDictionary<string, string>> FetchPageNamesAndStore(NotionClient client, List<string> pageIds, string itemType, List<string> nameList, string Group)
        {
	        var logger = LoggerFactory.CreateLogger("console");
	        ConcurrentDictionary<string, string> pageNameDict = new();

	        var tasks = pageIds.Select(async id =>
	        {
		        var page = await RetryWithBackoff(() => client.Pages.RetrieveAsync(id));

		        if (page.Properties.TryGetValue("Internal Name", out PropertyValue nameValue) && nameValue is TitlePropertyValue titleProperty)
		        {
			        string nameText = titleProperty.Title.FirstOrDefault()?.PlainText ?? "Unnamed";
			        if (nameList != null) nameList.Add(nameText);
			        pageNameDict[nameText] = id;

			        // Log each retrieved page for debugging
			        logger.Info($"[{Group}]: Retrieved {itemType} Name: {nameText} (ID: {id})");
		        }
		        else
		        {
			        // Log any failed attempts to retrieve data
			        logger.Warning($"[{Group}]: Failed to retrieve {itemType} Name for ID: {id}");
		        }
	        }).ToList();

	        await Task.WhenAll(tasks);
	        return pageNameDict;
        }
    }
}