import unittest

from polymarket_inspector.analytics import build_market_catalog, bucket_for_price, choose_category


class AnalyticsHelpersTests(unittest.TestCase):
    def test_choose_category_prefers_first_specific_tag(self):
        category, labels = choose_category(
            {
                "title": "Example event",
                "tags": [
                    {"label": "All"},
                    {"label": "Politics"},
                    {"label": "US Election"},
                ],
            }
        )
        self.assertEqual(category, "Politics")
        self.assertIn("Politics", labels)

    def test_choose_category_falls_back_to_title_heuristic(self):
        category, _ = choose_category(None, "Bitcoin up or down today?")
        self.assertEqual(category, "Crypto")

    def test_bucket_for_price(self):
        self.assertEqual(bucket_for_price(0.02), "0.00-0.05")
        self.assertEqual(bucket_for_price(0.73), "0.60-0.80")
        self.assertEqual(bucket_for_price(0.98), "0.95-1.00")

    def test_build_market_catalog_extracts_outcomes_and_tags(self):
        catalog = build_market_catalog(
            {
                "example-event": {
                    "title": "Will it rain tomorrow?",
                    "tags": [{"label": "Weather"}],
                    "closed": True,
                    "markets": [
                        {
                            "conditionId": "0xabc",
                            "question": "Will it rain tomorrow?",
                            "outcomes": '["Yes", "No"]',
                            "outcomePrices": '["1", "0"]',
                            "umaResolutionStatus": "resolved",
                            "endDate": "2026-05-01T00:00:00Z",
                        }
                    ],
                }
            }
        )
        market = catalog["0xabc"]
        self.assertEqual(market["category"], "Weather")
        self.assertTrue(market["resolved"])
        self.assertEqual(market["outcomePrices"]["Yes"], 1.0)
        self.assertEqual(market["outcomePrices"]["No"], 0.0)


if __name__ == "__main__":
    unittest.main()

