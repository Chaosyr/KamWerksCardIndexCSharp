using Notion.Client;
using KamWerksCardIndexCSharp.Notion.Helper_Methods;
using RichTextProperty = KamWerksCardIndexCSharp.Notion.Helper_Methods.RichTextProperty;
using SelectProperty = KamWerksCardIndexCSharp.Notion.Helper_Methods.SelectProperty;
using TitleProperty = KamWerksCardIndexCSharp.Notion.Helper_Methods.TitleProperty;
using MultiSelectProperty = KamWerksCardIndexCSharp.Notion.Helper_Methods.MultiSelectProperty;


namespace KamWerksCardIndexCSharp.Notion.SendToGitAsJSON.helpers;

public class PrepareJSON
{
    public static async Task<string> PrepareJSONFile(Page page, string type)
    {
        Dictionary<string, PropertyValue> Properties = new Dictionary<string, PropertyValue>();
	    List<string> properties = new List<string>();
	    List<string> propertiesEarly = new List<string>();
	    
	    string jsonRet = "";
		if (type == "Card")
		{
			page.Properties.TryGetValue("Internal Name", out var internalName);
			page.Properties.TryGetValue("From", out var from);
			Properties.Add("Internal Name", internalName);
			Properties.Add("From", from);
			
			foreach (var property in Properties)
			{
				if (property.Value.Type == PropertyValueType.Title)
				{
					var titleProperty = await TitleProperty.GetPropertyAsString(property.Value);
					propertiesEarly.Add(titleProperty);
				}
				if (property.Value.Type == PropertyValueType.RichText)
				{
					var richTextProperty = await RichTextProperty.GetPropertyAsString(property.Value);
					propertiesEarly.Add(richTextProperty);
				}
				if (property.Value.Type == PropertyValueType.Select)
				{
					var selectProperty = await SelectProperty.GetPropertyAsString(property.Value);
					propertiesEarly.Add(selectProperty);
				}
				if (property.Value.Type == PropertyValueType.MultiSelect)
				{
					var multiSelectProperty = await MultiSelectProperty.GetPropertyAsString(property.Value);
					properties.Add(multiSelectProperty);
				}
				if (property.Value.Type == PropertyValueType.Url)
				{
					var urlProperty = await URLProperty.GetPropertyAsString(property.Value);
					propertiesEarly.Add(urlProperty);
				}
			}

			if (propertiesEarly[1] == "Custom TCG Inscryption")
			{
				// Get Propeties for List
				page.Properties.TryGetValue("Name", out var notionName);
				page.Properties.TryGetValue("Temple", out var temple);
				page.Properties.TryGetValue("Rarity", out var rarity);
				page.Properties.TryGetValue("Cost", out var cost);
				page.Properties.TryGetValue("Power", out var power);
				page.Properties.TryGetValue("Health", out var health);
				page.Properties.TryGetValue("Flavor", out var flavor);
				page.Properties.TryGetValue("Token", out var token);
				page.Properties.TryGetValue("Sigil 1", out var sigil1);
				page.Properties.TryGetValue("Sigil 2", out var sigil2);
				page.Properties.TryGetValue("Sigil 3", out var sigil3);
				page.Properties.TryGetValue("Sigil 4", out var sigil4);
				page.Properties.TryGetValue("Artist", out var artist);
				page.Properties.TryGetValue("Wiki-Page", out var wikiPage);
				page.Properties.TryGetValue("Tags", out var tags);
				// Add properties to List
				Properties.Add("Notion Name", notionName);
				Properties.Add("Temple", temple);
				Properties.Add("Rarity", rarity);
				Properties.Add("Cost", cost);
				Properties.Add("Power", power);
				Properties.Add("Health", health);
				Properties.Add("Flavor", flavor);
				Properties.Add("Token", token);
				Properties.Add("Sigil 1", sigil1);
				Properties.Add("Sigil 2", sigil2);
				Properties.Add("Sigil 3", sigil3);
				Properties.Add("Sigil 4", sigil4);
				Properties.Add("Artist", artist);
				Properties.Add("Wiki-Page", wikiPage);
				Properties.Add("Tags", tags);

				foreach (var property in Properties)
				{
					if (property.Value.Type == PropertyValueType.Title)
					{
						var titleProperty = await TitleProperty.GetPropertyAsString(property.Value);
						properties.Add(titleProperty);
					}

					if (property.Value.Type == PropertyValueType.RichText)
					{
						var richTextProperty = await RichTextProperty.GetPropertyAsString(property.Value);
						properties.Add(richTextProperty);
					}

					if (property.Value.Type == PropertyValueType.Select)
					{
						var selectProperty = await SelectProperty.GetPropertyAsString(property.Value);
						properties.Add(selectProperty);
					}
					
					if (property.Value.Type == PropertyValueType.MultiSelect)
					{
						var multiSelectProperty = await MultiSelectProperty.GetPropertyAsString(property.Value);
						properties.Add(multiSelectProperty);
					}

					if (property.Value.Type == PropertyValueType.Url)
					{
						var urlProperty = await URLProperty.GetPropertyAsString(property.Value);
						properties.Add(urlProperty);
					}
				}

				jsonRet = $$"""
				             {
				                 "internalName": "{{properties[0]}}",
				                 "from": "{{properties[1]}}",
				                 "displayName": "{{properties[2]}}",
				                 "temple": "{{properties[3]}}",
				                 "rarity": "{{properties[4]}}",
				                 "cost": "{{properties[5]}}",
				                 "power": "{{properties[6]}}",
				                 "health": "{{properties[7]}}",
				                 "flavor": "{{properties[8]}}",
				                 "token": "{{properties[9]}}",
				                 "sigils": [
				             """;

				List<string> sigils = new List<string>();
				
				if (properties[10] != "")
				{
					sigils.Add(properties[10]);
				}
				if (properties[11] != "")
				{
					sigils.Add(properties[11]);
				}
				if (properties[12] != "")
				{
					sigils.Add(properties[12]);
				}
				if (properties[13] != "")
				{
					sigils.Add(properties[13]);
				}

				foreach (string sig in sigils)
				{
					if (string.IsNullOrEmpty(sig)) continue;
					jsonRet += $"\n        \"{sig}\",";
				}
				jsonRet = jsonRet.TrimEnd(',') + "\n";

				jsonRet += $$"""
				                 ],
				                 "artist": "{{properties[14]}}",
				                 "wiki-page": "{{properties[15]}}",
				                 "tags": [
				             """;
				foreach (string tag in properties[16].Split(" "))
				{
					if (string.IsNullOrEmpty(tag)) continue;
					jsonRet += $"\n        \"{tag}\",";
				}
				jsonRet = jsonRet.TrimEnd(',') + "\n";
				jsonRet += $$"""
				                 ]
				             }
				             """;
			} else if (propertiesEarly[1] == "Desaft’s Mod (CTI)")
			{
				// Get Propeties for List
				page.Properties.TryGetValue("Name", out var notionName);
				page.Properties.TryGetValue("Temple", out var temple);
				page.Properties.TryGetValue("Rarity", out var rarity);
				page.Properties.TryGetValue("Tribes", out var tribes);
				page.Properties.TryGetValue("Cost", out var cost);
				page.Properties.TryGetValue("Power", out var power);
				page.Properties.TryGetValue("Health", out var health);
				page.Properties.TryGetValue("Flavor", out var flavor);
				page.Properties.TryGetValue("Token", out var token);
				page.Properties.TryGetValue("Sigil 1", out var sigil1);
				page.Properties.TryGetValue("Sigil 2", out var sigil2);
				page.Properties.TryGetValue("Sigil 3", out var sigil3);
				page.Properties.TryGetValue("Sigil 4", out var sigil4);
				page.Properties.TryGetValue("Artist", out var artist);
				page.Properties.TryGetValue("Wiki-Page", out var wikiPage);
				page.Properties.TryGetValue("Tags", out var tags);
				// Add properties to List
				Properties.Add("Notion Name", notionName);
				Properties.Add("Temple", temple);
				Properties.Add("Rarity", rarity);
				Properties.Add("Tribes", tribes);
				Properties.Add("Cost", cost);
				Properties.Add("Power", power);
				Properties.Add("Health", health);
				Properties.Add("Flavor", flavor);
				Properties.Add("Token", token);
				Properties.Add("Sigil 1", sigil1);
				Properties.Add("Sigil 2", sigil2);
				Properties.Add("Sigil 3", sigil3);
				Properties.Add("Sigil 4", sigil4);
				Properties.Add("Artist", artist);
				Properties.Add("Wiki-Page", wikiPage);
				Properties.Add("Tags", tags);

				foreach (var property in Properties)
				{
					if (property.Value.Type == PropertyValueType.Title)
					{
						var titleProperty = await TitleProperty.GetPropertyAsString(property.Value);
						properties.Add(titleProperty);
					}

					if (property.Value.Type == PropertyValueType.RichText)
					{
						var richTextProperty = await RichTextProperty.GetPropertyAsString(property.Value);
						properties.Add(richTextProperty);
					}

					if (property.Value.Type == PropertyValueType.Select)
					{
						var selectProperty = await SelectProperty.GetPropertyAsString(property.Value);
						properties.Add(selectProperty);
					}
					
					if (property.Value.Type == PropertyValueType.MultiSelect)
					{
						var multiSelectProperty = await MultiSelectProperty.GetPropertyAsString(property.Value);
						properties.Add(multiSelectProperty);
					}

					if (property.Value.Type == PropertyValueType.Url)
					{
						var urlProperty = await URLProperty.GetPropertyAsString(property.Value);
						properties.Add(urlProperty);
					}
				}

				jsonRet = $$"""
				            {
				                "internalName": "{{properties[0]}}",
				                "from": "{{properties[1]}}",
				                "displayName": "{{properties[2]}}",
				                "temple": "{{properties[3]}}",
				                "rarity": "{{properties[4]}}",
				                "tribes": [
				            """;
				
				foreach (string trb in properties[5].Split(" "))
				{
					if (string.IsNullOrEmpty(trb)) continue;
					jsonRet += $"\n        \"{trb}\",";
				}
				jsonRet = jsonRet.TrimEnd(',') + "\n";
				
				jsonRet += $$"""
				                 ]
				                 "cost": "{{properties[6]}}",
				                 "power": "{{properties[7]}}",
				                 "health": "{{properties[8]}}",
				                 "flavor": "{{properties[9]}}",
				                 "token": "{{properties[10]}}",
				                 "sigils": [
				             """;

				List<string> sigils = new List<string>();
				
				if (properties[11] != "")
				{
					sigils.Add(properties[11]);
				}
				if (properties[12] != "")
				{
					sigils.Add(properties[12]);
				}
				if (properties[13] != "")
				{
					sigils.Add(properties[13]);
				}
				if (properties[14] != "")
				{
					sigils.Add(properties[14]);
				}

				foreach (string sig in sigils)
				{
					if (string.IsNullOrEmpty(sig)) continue;
					jsonRet += $"\n        \"{sig}\",";
				}
				jsonRet = jsonRet.TrimEnd(',') + "\n";

				jsonRet += $$"""
				                 ],
				                 "artist": "{{properties[15]}}",
				                 "wiki-page": "{{properties[16]}}",
				                 "tags": [
				             """;
				foreach (string tag in properties[17].Split(" "))
				{
					if (string.IsNullOrEmpty(tag)) continue;
					jsonRet += $"\n        \"{tag}\",";
				}
				jsonRet = jsonRet.TrimEnd(',') + "\n";
				jsonRet += $$"""
				                 ]
				             }
				             """;
			} else if (propertiesEarly[1] == "Inscryption Overhaul")
			{
				// Get Propeties for List
				page.Properties.TryGetValue("Name", out var notionName);
				page.Properties.TryGetValue("Temple", out var temple);
				page.Properties.TryGetValue("Rarity", out var rarity);
				page.Properties.TryGetValue("Cost", out var cost);
				page.Properties.TryGetValue("Power", out var power);
				page.Properties.TryGetValue("Health", out var health);
				page.Properties.TryGetValue("Flavor", out var flavor);
				page.Properties.TryGetValue("Token", out var token);
				page.Properties.TryGetValue("Sigil 1", out var sigil1);
				page.Properties.TryGetValue("Sigil 2", out var sigil2);
				page.Properties.TryGetValue("Sigil 3", out var sigil3);
				page.Properties.TryGetValue("Sigil 4", out var sigil4);
				page.Properties.TryGetValue("Artist", out var artist);
				page.Properties.TryGetValue("Wiki-Page", out var wikiPage);
				page.Properties.TryGetValue("Tags", out var tags);
				// Add properties to List
				Properties.Add("Notion Name", notionName);
				Properties.Add("Temple", temple);
				Properties.Add("Rarity", rarity);
				Properties.Add("Cost", cost);
				Properties.Add("Power", power);
				Properties.Add("Health", health);
				Properties.Add("Flavor", flavor);
				Properties.Add("Token", token);
				Properties.Add("Sigil 1", sigil1);
				Properties.Add("Sigil 2", sigil2);
				Properties.Add("Sigil 3", sigil3);
				Properties.Add("Sigil 4", sigil4);
				Properties.Add("Artist", artist);
				Properties.Add("Wiki-Page", wikiPage);
				Properties.Add("Tags", tags);

				foreach (var property in Properties)
				{
					if (property.Value.Type == PropertyValueType.Title)
					{
						var titleProperty = await TitleProperty.GetPropertyAsString(property.Value);
						properties.Add(titleProperty);
					}

					if (property.Value.Type == PropertyValueType.RichText)
					{
						var richTextProperty = await RichTextProperty.GetPropertyAsString(property.Value);
						properties.Add(richTextProperty);
					}

					if (property.Value.Type == PropertyValueType.Select)
					{
						var selectProperty = await SelectProperty.GetPropertyAsString(property.Value);
						properties.Add(selectProperty);
					}
					
					if (property.Value.Type == PropertyValueType.MultiSelect)
					{
						var multiSelectProperty = await MultiSelectProperty.GetPropertyAsString(property.Value);
						properties.Add(multiSelectProperty);
					}

					if (property.Value.Type == PropertyValueType.Url)
					{
						var urlProperty = await URLProperty.GetPropertyAsString(property.Value);
						properties.Add(urlProperty);
					}
				}

				jsonRet = $$"""
				             {
				                 "internalName": "{{properties[0]}}",
				                 "from": "{{properties[1]}}",
				                 "displayName": "{{properties[2]}}",
				                 "temple": "{{properties[3]}}",
				                 "rarity": "{{properties[4]}}",
				                 "cost": "{{properties[5]}}",
				                 "power": "{{properties[6]}}",
				                 "health": "{{properties[7]}}",
				                 "flavor": "{{properties[8]}}",
				                 "token": "{{properties[9]}}",
				                 "sigils": [
				             """;

				List<string> sigils = new List<string>();
				
				if (properties[10] != "")
				{
					sigils.Add(properties[10]);
				}
				if (properties[11] != "")
				{
					sigils.Add(properties[11]);
				}
				if (properties[12] != "")
				{
					sigils.Add(properties[12]);
				}
				if (properties[13] != "")
				{
					sigils.Add(properties[13]);
				}

				foreach (string sig in sigils)
				{
					if (string.IsNullOrEmpty(sig)) continue;
					jsonRet += $"\n        \"{sig}\",";
				}
				jsonRet = jsonRet.TrimEnd(',') + "\n";

				jsonRet += $$"""
				                 ],
				                 "artist": "{{properties[14]}}",
				                 "wiki-page": "{{properties[15]}}",
				                 "tags": [
				             """;
				foreach (string tag in properties[16].Split(" "))
				{
					if (string.IsNullOrEmpty(tag)) continue;
					jsonRet += $"\n        \"{tag}\",";
				}
				jsonRet = jsonRet.TrimEnd(',') + "\n";
				jsonRet += $$"""
				                 ]
				             }
				             """;
			}
		}
		
		if (type == "Sigil")
		{
			// Get Propeties for List
			page.Properties.TryGetValue("Internal Name", out var internalName);
			page.Properties.TryGetValue("Name", out var namesigil);
			page.Properties.TryGetValue("Description", out var description);
			page.Properties.TryGetValue("Category", out var category);
			// Add properties to List
			Properties.Add("Internal Name", internalName);
			Properties.Add("Name", namesigil);
			Properties.Add("Description", description);
			Properties.Add("Category", category);
			foreach (var property in Properties)
			{
				if (property.Value.Type == PropertyValueType.Title)
				{
					var titleProperty = await TitleProperty.GetPropertyAsString(property.Value);
					properties.Add(titleProperty);
				}
				if (property.Value.Type == PropertyValueType.RichText)
				{
					var richTextProperty = await RichTextProperty.GetPropertyAsString(property.Value);
					properties.Add(richTextProperty);
				}
				if (property.Value.Type == PropertyValueType.Select)
				{
					var selectProperty = await SelectProperty.GetPropertyAsString(property.Value);
					properties.Add(selectProperty);
				}
			}

			jsonRet = $$"""
			            {
			                "internalName": "{{properties[0]}}",
			                "name": "{{properties[1]}}",
			                "description": "{{properties[2]}}",
			                "category": "{{properties[3]}}"
			            }
			            """;
		}
		Console.WriteLine($"Printing out Object of Type {type}.");
		Console.WriteLine(jsonRet);
		return jsonRet;
    }
}