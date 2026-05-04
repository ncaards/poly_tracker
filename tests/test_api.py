import unittest

from polymarket_inspector.api import coerce_items, format_timestamp, is_wallet_address, summarize_trades


class ApiHelpersTests(unittest.TestCase):
    def test_is_wallet_address(self):
        self.assertTrue(is_wallet_address("0x1234567890abcdef1234567890abcdef12345678"))
        self.assertFalse(is_wallet_address("0x1234"))
        self.assertFalse(is_wallet_address("preparedminds"))

    def test_coerce_items_accepts_list_and_wrapped_value(self):
        payload_list = [{"a": 1}, {"b": 2}]
        payload_wrapped = {"value": [{"c": 3}]}
        self.assertEqual(coerce_items(payload_list), payload_list)
        self.assertEqual(coerce_items(payload_wrapped), [{"c": 3}])

    def test_summarize_trades(self):
        summary = summarize_trades(
            [
                {"side": "BUY", "size": 10, "price": 0.4, "timestamp": 100},
                {"side": "SELL", "size": 5, "price": 0.8, "timestamp": 200},
                {"side": "BUY", "size": 2, "price": 0.5, "timestamp": 150},
            ]
        )
        self.assertEqual(summary["count"], 3)
        self.assertEqual(summary["buy_count"], 2)
        self.assertEqual(summary["sell_count"], 1)
        self.assertEqual(summary["buy_notional"], 5.0)
        self.assertEqual(summary["sell_notional"], 4.0)
        self.assertEqual(summary["net_notional"], 1.0)
        self.assertEqual(summary["last_trade_at"], 200)

    def test_format_timestamp_handles_none(self):
        self.assertEqual(format_timestamp(None), "-")


if __name__ == "__main__":
    unittest.main()

