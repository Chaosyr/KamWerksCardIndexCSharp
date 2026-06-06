using DSharpPlus.Commands;
using DSharpPlus.Entities;
using KamWerksCardIndexCSharp.Helpers;
using KamWerksCardIndexCSharp.Notion;
using KamWerksCardIndexCSharp.Notion.SendToGitAsJSON;

namespace KamWerksCardIndexCSharp.DiscordBot.CommandBases
{
	public class Admin_Commands
	{
		public static async Task AdminAsyncCommands(CommandContext context)
		{
			var logger = LoggerFactory.CreateLogger("console");
			var message = context.Command.ToString();

			if (message.Replace("|", "").Contains("Recache"))
			{
				await context.RespondAsync("Running Recache, please wait until the next message is sent.");
				
				var activity = new DiscordActivity
				{
					Name = "Loading up Notion Databases, please Hold!",
					ActivityType = DiscordActivityType.Streaming
				};
				
				await context.Client.UpdateStatusAsync(activity, DiscordUserStatus.Idle);
				
				await NotionEnd.NotionMain(context.Client);
				
				var activity3 = new DiscordActivity
				{
					Name = "Checking and Sending Updated Databases to GitHub!",
					ActivityType = DiscordActivityType.Streaming
				};
				
				await context.Client.UpdateStatusAsync(activity3, DiscordUserStatus.Idle);
				
				logger.Info("Sending data to GitHub Real Quick!");
				await SendToGit.SendOff(NotionEnd.JSONCollection);
				logger.Info("Finished Sending, returned to main loop!");
				
				var activity2 = new DiscordActivity
				{
					Name = "Monitoring for Commands!",
					ActivityType = DiscordActivityType.Playing
				};
			
				await context.Client.UpdateStatusAsync(activity2, DiscordUserStatus.Idle);
				var messageOutput = "Admin Command: " + "Recache" + " has completed";
				await context.FollowupAsync(messageOutput);
			}
			return;
		}
	}
}