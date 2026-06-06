using KamWerksCardIndexCSharp.DiscordBot;
using Octokit;

namespace KamWerksCardIndexCSharp.API_Github.Helpers
{
	public class FillGithubReposClass
	{
		/// <summary>
		/// This is the valid repos in which we will query from.
		/// </summary>
		/// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
		public static List<string> GithubReposToGoThrough = new List<string>()
		{
			"Chaosyr/NotionAssets",
			"Chaosyr/KamWerksPortraitsAndOtherAssets",
		};
		/// <summary>
		/// This checks if the repository is valid and adds it to GithubRepos.
		/// </summary>
		/// <returns>A repository added to GithubRepos.</returns>
		/// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
		public static async Task FillGithubRepos()
		{
			foreach (string repo in GithubReposToGoThrough)
			{
				try
				{
					Repository repository = await DiscordEnd.GitHubClientPublic.Repository.Get(repo.Split('/')[0], repo.Split('/')[1]);
					Console.WriteLine("FillGithubRepos-" + "Info: " + $"Adding {repository.FullName} to GithubRepos.");
					DiscordEnd.GithubRepos.Add(repository);
				}
				catch
				{
					Console.WriteLine("FillGithubRepos-" + "Error: " + $"Could not add {repo} to GithubRepos, validate that the entry is correct.");
				}
			}
		}
	}
}