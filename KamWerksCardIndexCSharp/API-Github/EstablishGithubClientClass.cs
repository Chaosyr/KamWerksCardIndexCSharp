using KamWerksCardIndexCSharp.DiscordBot;
using Octokit;

namespace KamWerksCardIndexCSharp.API_Github
{
	public class EstablishGithubClientClass
	{
		/// <summary>
		/// Establishes the Github Client.
		/// </summary>
		/// <returns>An Established Github Client</returns>
		/// <remarks>This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.</remarks>
		public static async Task EstablishGithubClient()
		{
			Console.WriteLine("EstablishGithubClient-" + "Info: " + "Setting up the GitHub Client.");
			GitHubClient client = new GitHubClient(new ProductHeaderValue("KamWerksCardIndex"));
			client.Credentials = new Credentials(Environment.GetEnvironmentVariable("GITHUB_KAM_TOKEN"));
			DiscordEnd.GitHubClientPublic = client;
		}
	}
}