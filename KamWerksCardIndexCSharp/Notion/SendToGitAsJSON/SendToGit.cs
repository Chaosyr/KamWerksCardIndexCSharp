using KamWerksCardIndexCSharp.API_Github.Helpers;
using KamWerksCardIndexCSharp.DiscordBot;
using KamWerksCardIndexCSharp.Helpers.GithubNodes;
using Octokit;
namespace KamWerksCardIndexCSharp.Notion.SendToGitAsJSON;

public class SendToGit
{
    public static async Task SendOff(List<(string set, string type, List<string> json)> JSONMaster)
    {
        foreach ((string set, string type, List<string> json) item in JSONMaster)
        {
            string setProper = "";
            switch (item.set)
            {
                case "CTI":
                {
                    setProper = "Custom TCG Inscryption";
                    break;
                }
                case "DMC":
                {
                    setProper = "Desafts Mod (CTI)";
                    break;
                }
                case "IOTFD":
                {
                    setProper = "Inscryption Overhaul - Final Duel Edition";
                    break;
                }
            } 
            string master = $$"""
                            {
                                "format": "{{item.set}}",
                                "type": "{{item.type}}",
                                "items": [
                            """;
            
            foreach (string s in item.json)
            {
                string s2 = s + ",";
                foreach (string line in s2.Split("\n"))
                {
                    master += $"\n        {line}";
                }
            }
            
            master = master.TrimEnd(',') + "\n";

            master += $$"""
                            ]
                        }
                        """;
            
            
            if (item.type == "Card")
            {
                GitHubClient client = DiscordEnd.GitHubClientPublic;
                Repository repository = DiscordEnd.GithubRepos.FirstOrDefault(repo => repo.FullName.Equals("Chaosyr/NotionAssets"));
                List<RepositoryContent> contents = DiscordEnd.GithubRepoContents[repository.FullName];
                
                (bool found1, GithubNode node) = await GetNodeInHeirarchy.GetWhere("Chaosyr/NotionAssets", contents, "Formats", setProper);
                if (!found1) { Console.WriteLine($"Could not find {setProper}, skipping {item.set} {item.type}."); continue; }

                (bool found2, GithubNode node2) = await GetNodeInHeirarchy.GetWhere("Chaosyr/NotionAssets", null, node.Current.Path, "Raw Sheets");
                if (!found2) { Console.WriteLine($"Could not find Raw Sheets, skipping {item.set} {item.type}."); continue; }
                
                string filePath = $"{node2.Current.Path}/Master-{item.set}-Cards-Sheet.json";

                try
                {
                    var existingFile = await client.Repository.Content.GetAllContents("Chaosyr", "NotionAssets", filePath);
                    string existingContent = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(existingFile[0].EncodedContent));
                    string existingNormalized = existingContent.Replace("\r\n", "\n").Trim();
                    string masterNormalized = master.Replace("\r\n", "\n").Trim();

                    if (existingNormalized == masterNormalized)
                    {
                        Console.WriteLine($"No changes for {filePath}, skipping.");
                    }
                    else
                    {
                        await client.Repository.Content.UpdateFile(
                            "Chaosyr", "NotionAssets", filePath,
                            new UpdateFileRequest($"Update {item.set} Cards Sheet", master, existingFile[0].Sha)
                        );
                    }
                }
                catch (NotFoundException)
                {
                    await client.Repository.Content.CreateFile(
                        "Chaosyr", "NotionAssets", filePath,
                        new CreateFileRequest($"Create {item.set} Cards Sheet", master)
                    );
                }
            }
            if (item.type == "Sigil")
            {
                GitHubClient client = DiscordEnd.GitHubClientPublic;
                Repository repository = DiscordEnd.GithubRepos.FirstOrDefault(repo => repo.FullName.Equals("Chaosyr/NotionAssets"));
                List<RepositoryContent> contents = DiscordEnd.GithubRepoContents[repository.FullName];

                (bool found1, GithubNode node) = await GetNodeInHeirarchy.GetWhere("Chaosyr/NotionAssets", contents, "Formats", setProper);
                if (!found1) { Console.WriteLine($"Could not find {setProper}, skipping {item.set} {item.type}."); continue; }

                (bool found2, GithubNode node2) = await GetNodeInHeirarchy.GetWhere("Chaosyr/NotionAssets", null, node.Current.Path, "Raw Sheets");
                if (!found2) { Console.WriteLine($"Could not find Raw Sheets, skipping {item.set} {item.type}."); continue; }
                
                string filePath = $"{node2.Current.Path}/Master-{item.set}-Sigils-Sheet.json";

                try
                {
                    var existingFile = await client.Repository.Content.GetAllContents("Chaosyr", "NotionAssets", filePath);
                    string existingContent = System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(existingFile[0].EncodedContent));
                    string existingNormalized = existingContent.Replace("\r\n", "\n").Trim();
                    string masterNormalized = master.Replace("\r\n", "\n").Trim();

                    if (existingNormalized == masterNormalized)
                    {
                        Console.WriteLine($"No changes for {filePath}, skipping.");
                    }
                    else
                    {
                        await client.Repository.Content.UpdateFile(
                            "Chaosyr", "NotionAssets", filePath,
                            new UpdateFileRequest($"Update {item.set} Sigils Sheet", master, existingFile[0].Sha)
                        );
                    }
                }
                catch (NotFoundException)
                {
                    await client.Repository.Content.CreateFile(
                        "Chaosyr", "NotionAssets", filePath,
                        new CreateFileRequest($"Create {item.set} Cards Sheet", master)
                    );
                }
            }
        }
        
        return;
    }
}