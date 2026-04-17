import json, sys
sys.path.insert(0, '.')
from data_sources.apify_collector import ApifyCollector

item = {
    "id": 5998827,
    "name": "Jack Daniel's Old No.7 1 л",
    "image": "https://example.com/img.jpg",
    "url": "https://rozetka.com.ua/ua/jack_daniels/p5998827/",
    "price": {"current": 979, "old": 1199},
    "reviews": {"count": 293, "rating": 4.1},
    "brand": {"id": 115425, "name": "Jack Daniel's"},
    "seller": {"id": 5, "name": "Rozetka"},
    "category": {"id": 4649130, "name": "Віскі", "root": "Алкогольні напої та продукти харчування"}
}

collector = ApifyCollector.__new__(ApifyCollector)
parsed = collector._safe_parse_rozetka(item, "alcohol")
for k, v in parsed.items():
    print(f"{k}: {v}")

print()
detected = collector._detect_category(item)
print(f"Detected category: {detected}")
