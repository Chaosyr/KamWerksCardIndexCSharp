using KamWerksCardIndexCSharp.DiscordBot;
using Octokit;

namespace KamWerksCardIndexCSharp.Helpers.GithubNodes.Helpers
{
	public class GetNodesClass
	{
		public static bool hasRanAlready = false;
		/// <summary>
		/// Gets the full content tree of the repository passed in and returns it.
		/// </summary>
		/// <param name="repo">The repository in which to get Content from.</param>
		/// <param name="parents">The base list of RepositoryContent in which will be used as the parent at depth 0.</param>
		/// <param name="altParent">The GithubNode that will be used as the Parent beyond depth 0.</param>
		/// <param name="path">The path in which to start retrieving from.</param>
		/// <param name="depth">The depth in which we are within the node.</param>
		/// <returns>A list of GithubNodes within the repo at path.</returns>
		/// This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.
		public static async Task<List<GithubNode>> GetNodes(Repository repo, List<RepositoryContent> parents, GithubNode altParent, string path, int depth)
		{
			List<GithubNode> nodes = new List<GithubNode>();
			List<RepositoryContent> result = new List<RepositoryContent>();
			GithubNode node = new GithubNode();
			if (depth == 0)
			{
				result = parents;
			}
			else
			{
				try
				{
					if (!path.Contains('.') && !path.Contains("LICENSE"))
					{
						IReadOnlyList<RepositoryContent> contents = await DiscordEnd.GitHubClientPublic.Repository.Content.GetAllContents(repo.FullName.Split('/')[0], repo.FullName.Split('/')[1], path);
						result = contents.ToList();
					}
				}
				catch
				{
					Console.WriteLine("EstablishGithubClient-" + "Error: " + $"Failed to read {repo.FullName} at {path}, ensure the folder path is valid, and if it is valid, this is firing because there is no children.");
					return nodes;
				}
			}

			foreach (RepositoryContent content in result)
			{
				string newPath = string.IsNullOrEmpty(path) ? content.Name : $"{path.TrimEnd('/')}/{content.Name}";
				
				if (hasRanAlready == false)
				{
					Console.WriteLine("EstablishGithubClient-" + "Info: " + $"Found {(String.IsNullOrWhiteSpace(path) ? "root" + path : "child in " + path + " at")} {content.Name}");
				}
				List<GithubNode> children = new List<GithubNode>();
				if (content.Type == ContentType.Dir)
				{
					children = await GetNodes(repo, result, altParent, newPath, depth + 1);
				}
				node = new GithubNode()
				{
					Parent = altParent, 
					Current = content, 
					Children = children,
				};
				nodes.Add(node);
			}
			return nodes;
		}
	}
}