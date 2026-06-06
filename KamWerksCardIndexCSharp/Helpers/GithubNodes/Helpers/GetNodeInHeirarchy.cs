using KamWerksCardIndexCSharp.DiscordBot;
using KamWerksCardIndexCSharp.Helpers.GithubNodes;
using Octokit;

namespace KamWerksCardIndexCSharp.API_Github.Helpers
{
    public class GetNodeInHeirarchy
    {
        public static async Task<(bool exists, GithubNode node)> GetWhere(string where, List<RepositoryContent> contents, string targetPath, string toEqual)
        {
            Repository repo = DiscordEnd.GithubRepos.FirstOrDefault(repository => repository.FullName.Equals(where));
            if (repo == null) return (false, new GithubNode());

            List<GithubNode> nodes = new List<GithubNode>();

            // If contents are null/empty, we are at the root level
            if (contents == null || contents.Count == 0)
            {
                if (DiscordEnd.GitHubAssets.TryGetValue(repo.FullName, out var assets))
                {
                    nodes = assets;
                }
                else
                {
                    return (false, new GithubNode());
                }
            }
            else
            {
                nodes = contents.Select(c => new GithubNode { Current = c }).ToList();
            }

            // Loop completely through all items at the current level
            foreach (GithubNode content in nodes)
            {
                // STEP 1: Check if this item is exactly the folder name we are looking for
                // AND ensure it sits inside the parent directory we specified
                if (content.Current.Name.Equals(toEqual) && content.Current.Path.StartsWith(targetPath))
                {
                    return (true, content);
                }

                // STEP 2: If we haven't reached our parent path yet, we need to dive into subfolders.
                // We open any directory that helps us get closer to the parent path.
                bool isParentOfTarget = targetPath.StartsWith(content.Current.Path);
                bool isInsideTarget = content.Current.Path.StartsWith(targetPath);

                if (content.Current.Type == ContentType.Dir && (isParentOfTarget || isInsideTarget))
                {
                    string[] repoParts = repo.FullName.Split('/');
                    
                    try
                    {
                        IReadOnlyList<RepositoryContent> subContents = await DiscordEnd.GitHubClientPublic.Repository.Content.GetAllContents(
                            repoParts[0], 
                            repoParts[1], 
                            content.Current.Path
                        );

                        // Recurse into the folder
                        var (subExists, subNode) = await GetWhere(where, subContents.ToList(), targetPath, toEqual);
                        if (subExists)
                        {
                            return (subExists, subNode); // Found it deeper down, bubble it up!
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"GetWhere-Error: {ex.Message}");
                    }
                }
            }

            // If the loop finishes completely and finds nothing at this level
            return (false, new GithubNode());
        }
    }
}

