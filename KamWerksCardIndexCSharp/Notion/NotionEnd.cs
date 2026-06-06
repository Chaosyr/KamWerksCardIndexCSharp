using DSharpPlus;
using DSharpPlus.Exceptions;
using Notion.Client;
using KamWerksCardIndexCSharp.Helpers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using KamWerksCardIndexCSharp.API_Github;
using KamWerksCardIndexCSharp.API_Github.Helpers;
using KamWerksCardIndexCSharp.DiscordBot;
using KamWerksCardIndexCSharp.Github;
using KamWerksCardIndexCSharp.Notion.SendToGitAsJSON.helpers;

namespace KamWerksCardIndexCSharp.Notion
{
	/// <summary>
	/// The Root of the NotionEnd of this Bot.
	/// </summary>
	/// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
    internal class NotionEnd
    {
	    /// <summary>
	    /// Custom TCG Inscryption Card Names List.
	    /// </summary>
	    /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> CtiCardNames { get; private set; } = new();
	    /// <summary>
	    /// Custom TCG Inscryption Sigil Names List.
	    /// </summary>
	    /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> CtiSigilNames { get; private set; } = new();
	    /// <summary>
	    /// Desafts Mod (CTI) Card Names List.
	    /// </summary>
	    /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> DmcCardNames { get; private set; } = new();
	    /// <summary>
	    /// Desafts Mod (CTI) Sigil Names List.
	    /// </summary>
	    /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> DmcSigilNames { get; private set; } = new();
	    /// <summary>
	    /// Inscryption Overhaul - Final Duel Edition Card Names List.
	    /// </summary>
	    /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> IotfdCardNames { get; private set; } = new();
	    /// <summary>
	    /// Inscryption Overhaul - Final Duel Edition Sigil Names List.
	    /// </summary>
	    /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> IotfdSigilNames { get; private set; } = new();
        
        /// <summary>
        /// Custom TCG Inscryption Cards Name -> ID associations.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static ConcurrentDictionary<string, string> CtiCards { get; private set; } = new();
        /// <summary>
        /// Custom TCG Inscryption Sigils Name -> ID associations.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static ConcurrentDictionary<string, string> CtiSigils { get; private set; } = new();
        /// <summary>
        /// Desafts Mod (CTI) Cards Name -> ID associations.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static ConcurrentDictionary<string, string> DmcCards { get; private set; } = new();
        /// <summary>
        /// Desafts Mod (CTI) Sigils Name -> ID associations.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static ConcurrentDictionary<string, string> DmcSigils { get; private set; } = new();
        /// <summary>
        /// Inscryption Overhaul - The Final Duel Cards Name -> ID associations.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static ConcurrentDictionary<string, string> IotfdCards { get; private set; } = new();
        /// <summary>
        /// Inscryption Overhaul - The Final Duel Sigils Name -> ID associations.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static ConcurrentDictionary<string, string> IotfdSigils { get; private set; } = new();
        
        /// <summary>
        /// Custom TCG Inscryption Cards JSONs.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> CtiMasterCardsJSON = new List<string>();
        
        /// <summary>
        /// Custom TCG Inscryption Sigils JSONs.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> CtiMasterSigilsJSON = new List<string>();
        
        /// <summary>
        /// Desafts Mod (CTI) Cards JSONs.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> DmcMasterCardsJSON = new List<string>();
        
        /// <summary>
        /// Desafts Mod (CTI) Sigils JSONs.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> DmcMasterSigilsJSON = new List<string>();
        
        /// <summary>
        /// Inscryption Overhaul - The Final Duel Cards JSONs.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> IotfdMasterCardsJSON = new List<string>();
        
        /// <summary>
        /// Inscryption Overhaul - The Final Duel Sigils JSONs.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<string> IotfdMasterSigilsJSON = new List<string>();
        
        /// <summary>
        /// The Master Set of JSONs to be sent to the Git of your choice.
        /// </summary>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static List<(string set, string type, List<string> json)> JSONCollection = new List<(string set, string type, List<string> json)>();

        /// <summary>
        /// The MAIN Method of the NotionEnd of this bot.
        /// </summary>
        /// <param name="discordClient">Your Discord Client.</param>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static async Task NotionMain(DiscordClient discordClient)
        {
	        CtiCardNames.Clear();
	        CtiSigilNames.Clear();
	        DmcCardNames.Clear();
	        DmcSigilNames.Clear();
	        IotfdCardNames.Clear();
	        IotfdSigilNames.Clear();
	        CtiMasterCardsJSON.Clear();
	        CtiMasterSigilsJSON.Clear();
	        DmcMasterCardsJSON.Clear();
	        DmcMasterSigilsJSON.Clear();
	        IotfdMasterCardsJSON.Clear();
	        IotfdMasterSigilsJSON.Clear();
	        JSONCollection.Clear();
	        
	        if (DiscordEnd.hasGithubRun != true) {
		        // Prepares GITHUB Environment for the bot.
		        EstablishGithubClientClass.EstablishGithubClient().Wait();
				FillGithubReposClass.FillGithubRepos().Wait();
				GetGitHubRepoContentsClass.GetGithubRepoContents().Wait();
				SetupGithubAssetsClass.SetupGithubAssets().Wait();
				DiscordEnd.hasGithubRun = true;
	        }

			var logger = LoggerFactory.CreateLogger("console");
			
			// Change NOTION_IPVP_TOKEN to match your NOTION API KEY, provided by Chaosyr
            string NotionAPIKey = Environment.GetEnvironmentVariable("NOTION_IPVP_TOKEN");
            if (string.IsNullOrWhiteSpace(NotionAPIKey))
            {
                logger.Error("Hey, You missed the Notion API Key Environment Var.");
                return;
            }

            logger.Info("Connecting to Notion API...");
            var client = NotionClientFactory.Create(new ClientOptions { AuthToken = NotionAPIKey });

            // These handle fetching and storing Notion Pages for the bot.
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
            
            // These will be sent to the JSON Master Sheet System.
            JSONCollection.Add(new ("CTI", "Card", CtiMasterCardsJSON));
            JSONCollection.Add(new ("CTI", "Sigil", CtiMasterSigilsJSON));
            JSONCollection.Add(new ("DMC", "Card", DmcMasterCardsJSON));
            JSONCollection.Add(new ("DMC", "Sigil", DmcMasterSigilsJSON));
            JSONCollection.Add(new ("IOTFD", "Card", IotfdMasterCardsJSON));
            JSONCollection.Add(new ("IOTFD", "Sigil", IotfdMasterSigilsJSON));

            logger.Info("Notion data retrieval completed successfully.");
        }
        
        /// <summary>
        /// Fetchs all of the Page ID's apart of the passed in DataSourceID, Group is utilized for logging.
        /// </summary>
        /// <param name="client">Your NOTION API Client</param>
        /// <param name="dataSourceId">The Notion Datasource ID you want to integrate.</param>
        /// <param name="Group">The Group in which this apart of e.g. Your Set's Code + Type.</param>
        /// <returns>Returns a List of String representing all of the IDs of the DataSource, this must be awaited.</returns>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
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

        /// <summary>
        /// This acts as a Backoff System for when the Bot gets Ratelimited if it happens, it will retry indefinitely until cancellation or until the Rate Limit is cleared or until all data is parsed.
        /// </summary>
        /// <param name="action">Represents the Task in which will be repeatedly tried.</param>
        /// <param name="ct">Represents the Cancellation Token.</param>
        /// <typeparam name="T">Represents the Return Type of your Task.</typeparam>
        /// <returns>What your Task normally would with handling for Ratelimits.</returns>
        /// <exception cref="UnreachableException">Thrown if 'ct.ThrowIfCancellationRequested()' was not Thrown.</exception>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        public static async Task<T> RetryWithBackoff<T>(
	        Func<Task<T>> action,
	        CancellationToken ct = default
        )
        {
	        int retryCount = 0;
	        int baseDelayMs = 1_000;
	        int maxDelayMs   = 30_000;

	        while (!ct.IsCancellationRequested)
	        {
		        try
		        {
			        return await action();
		        }
		        catch (NotionApiRateLimitException ex) when (ex.StatusCode == HttpStatusCode.TooManyRequests)
		        {
			        int waitTime = ex.RetryAfter?.Milliseconds + 50 ?? Math.Min(baseDelayMs * (int)Math.Pow(2, retryCount), maxDelayMs);

			        Console.WriteLine($"[Retry {retryCount}]: Notion rate limited! Retrying in {waitTime}ms...");
			        await Task.Delay(waitTime, ct);

			        retryCount++;
		        }
		        catch (DiscordException dex) when (
			        dex.Response?.StatusCode == HttpStatusCode.TooManyRequests
		        )
		        {
			        int waitTime = Math.Min(baseDelayMs * (int)Math.Pow(2, retryCount), maxDelayMs);

			        Console.WriteLine($"[Retry {retryCount}]: Discord rate limited! Retrying in {waitTime}ms...");
			        await Task.Delay(waitTime, ct);

			        retryCount++;
		        }
	        }

	        ct.ThrowIfCancellationRequested();
	        throw new UnreachableException();
        }

        /// <summary>
        /// This function handles setting up the ID to Name association for the bot, as well as setting up the JSONs to be sent to the GitHub of your choice.
        /// </summary>
        /// <param name="client">Your NOTION API Client.</param>
        /// <param name="pageIds">Your List of PageIds from FetchAllPageIds()</param>
        /// <param name="itemType">The type of Item this function is being called for can be 'Card' or 'Sigil', anything else will not be handled for JSONs.</param>
        /// <param name="nameList">The list of Names in which this function will append to.</param>
        /// <param name="Group">The Group in which this apart of e.g. Your Set's Code + Type.</param>
        /// <returns>A filled pageNameDict which will be returned directly as a ConcurrentDictionary, A filled nameList as passed into Params, and a filled JSONList; this one is currently hardcoded so you'll need to edit this function if you want that functionality.</returns>
        /// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
        private static async Task<ConcurrentDictionary<string, string>> FetchPageNamesAndStore(NotionClient client, List<string> pageIds, string itemType, List<string> nameList, string Group)
        {
	        var logger = LoggerFactory.CreateLogger("console");
	        ConcurrentDictionary<string, string> pageNameDict = new();

	        var semaphore = new SemaphoreSlim(2);

	        var tasks = pageIds.Select(async id =>
	        {
		        await semaphore.WaitAsync();
		        try
		        {
			        var page = await RetryWithBackoff(() => client.Pages.RetrieveAsync(id));

			        if (page.Properties.TryGetValue("Internal Name", out PropertyValue nameValue) && nameValue is TitlePropertyValue titleProperty)
			        {
				        string nameText = titleProperty.Title.FirstOrDefault()?.PlainText ?? "Unnamed";
				        if (nameList != null) nameList.Add(nameText);
				        pageNameDict[nameText] = id;

				        logger.Info($"[{Group}]: Retrieved {itemType} Name: {nameText} (ID: {id})");

				        string json = await PrepareJSON.PrepareJSONFile(page, itemType);
				        if (Group.Contains("CTI"))
				        {
					        if (itemType == "Card")
					        {
						        CtiMasterCardsJSON.Add(json);
					        }

					        if (itemType == "Sigil")
					        {
						        CtiMasterSigilsJSON.Add(json);
					        }
				        }
				        else if (Group.Contains("DMC"))
				        {
					        if (itemType == "Card")
					        {
						        DmcMasterCardsJSON.Add(json);
					        }

					        if (itemType == "Sigil")
					        {
						        DmcMasterSigilsJSON.Add(json);
					        }
				        }
				        else if (Group.Contains("IOTFD"))
				        {
					        if (itemType == "Card")
					        {
						        IotfdMasterCardsJSON.Add(json);
					        }

					        if (itemType == "Sigil")
					        {
						        IotfdMasterSigilsJSON.Add(json);
					        }
				        }
			        }
			        else
			        {
				        logger.Warning($"[{Group}]: Failed to retrieve {itemType} Name for ID: {id}");
			        }
		        }
		        finally
		        {
			        await Task.Delay(100);
			        semaphore.Release();
		        }
	        }).ToList();

	        await Task.WhenAll(tasks);
	        return pageNameDict;
        }
    }
}