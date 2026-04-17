from apify_client import ApifyClient
import json

client = ApifyClient("apify_api_h3WpjOuePhoMKdM4n1epspbG7ndiiW2YsUvL")
runs = client.actor("nazar/rozetka-category-scraper").runs().list(limit=1).items
if runs:
    items = client.dataset(runs[0]["defaultDatasetId"]).list_items(limit=3).items
    for item in items:
        print(json.dumps(item, ensure_ascii=False, indent=2))
        print("---")
else:
    print("No runs found")
