using Octokit;

namespace KamWerksCardIndexCSharp.Helpers.GithubNodes
{
	/// <summary>
	/// A setup node via Githubs System.
	/// </summary>
	/// This code is provided by Creator/Chaosyr/SaxbyMod/The Stoat Lord.
	public class GithubNode
	{
		public GithubNode Parent { get; set; }
		public RepositoryContent Current { get; set; } = new RepositoryContent();
		public List<GithubNode> Children { get; set; } = new List<GithubNode>();
	}
}