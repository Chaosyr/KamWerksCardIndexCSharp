using KamWerksCardIndexCSharp.API_Github.Helpers;
using KamWerksCardIndexCSharp.DiscordBot;
using KamWerksCardIndexCSharp.Helpers.GithubNodes;
using KamWerksCardIndexCSharp.Helpers.GithubNodes.Helpers;
using Octokit;

namespace KamWerksCardIndexCSharp.Github
{
	public class SetupGithubAssetsClass
	{
		/// <summary>
		/// This sets up the GithubAssets
		/// </summary>
		/// <returns>A set up GithubAssets.</returns>
		/// This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.
		public static async Task SetupGithubAssets()
		{
			foreach (string currentRepo in FillGithubReposClass.GithubReposToGoThrough)
			{
				GetNodesClass.hasRanAlready = false;
				Repository repository = DiscordEnd.GithubRepos.FirstOrDefault(repo => repo.FullName.Equals(currentRepo));
				List<RepositoryContent> parent = DiscordEnd.GithubRepoContents[repository.FullName];
				List<GithubNode> contents = await GetNodesClass.GetNodes(repository, parent, new GithubNode(), "", 0);
				DiscordEnd.GitHubAssets.Add(repository.FullName, contents);
				GetNodesClass.hasRanAlready = true;
			}
		}
	}
}