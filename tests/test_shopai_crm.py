import unittest
from datetime import datetime, timedelta

import pandas as pd

from FrontEnd.pages.shopai import build_shopai_conversation_frame, build_shopai_crm_summary


class TestShopAiCrm(unittest.TestCase):
    def test_conversation_frame_links_customers_by_phone_and_name(self):
        customers_df = pd.DataFrame(
            {
                "primary_name": ["Arif Rahman", "Nadia Islam"],
                "all_phones": ["01711234567", ""],
                "segment": ["VIP", "At Risk"],
                "total_orders": [6, 2],
                "total_revenue": [12000, 2500],
                "favorite_product": ["Blue Denim Jacket", "Printed Kurti Set"],
                "recency_days": [8, 75],
            }
        )
        conversations = [
            {
                "id": "1",
                "customer": "Arif Rahman",
                "customer_id": "+8801711234567",
                "platform": "whatsapp",
                "status": "open",
                "preview": "Where is my order?",
                "updated": datetime.now() - timedelta(minutes=5),
                "response_minutes": 2.0,
                "topic": "Order tracking",
                "tools": ["get_order_status"],
                "messages": [],
            },
            {
                "id": "2",
                "customer": "Nadia Islam",
                "customer_id": "nadia.islam",
                "platform": "instagram",
                "status": "escalated",
                "preview": "Damaged item refund",
                "updated": datetime.now() - timedelta(minutes=30),
                "response_minutes": 5.0,
                "topic": "Refund",
                "tools": ["create_refund"],
                "messages": [],
            },
        ]

        frame = build_shopai_conversation_frame(conversations=conversations, customers_df=customers_df)

        self.assertEqual(frame.loc[0, "segment"], "VIP")
        self.assertEqual(frame.loc[1, "segment"], "At Risk")
        self.assertEqual(frame.loc[0, "favorite_product"], "Blue Denim Jacket")
        self.assertEqual(frame.loc[1, "total_orders"], 2)

    def test_crm_summary_builds_attention_metrics(self):
        customers_df = pd.DataFrame(
            {
                "primary_name": ["Arif Rahman"],
                "all_phones": ["01711234567"],
                "segment": ["VIP"],
                "total_orders": [6],
                "total_revenue": [12000],
                "favorite_product": ["Blue Denim Jacket"],
                "recency_days": [8],
            }
        )
        conversations = [
            {
                "id": "1",
                "customer": "Arif Rahman",
                "customer_id": "+8801711234567",
                "platform": "whatsapp",
                "status": "escalated",
                "preview": "Refund issue",
                "updated": datetime.now() - timedelta(minutes=10),
                "response_minutes": 4.0,
                "topic": "Refund",
                "tools": ["create_refund", "escalate_to_human"],
                "messages": [],
            },
            {
                "id": "2",
                "customer": "Unknown Customer",
                "customer_id": "unknown",
                "platform": "messenger",
                "status": "resolved",
                "preview": "Product question",
                "updated": datetime.now() - timedelta(minutes=50),
                "response_minutes": 1.5,
                "topic": "Product discovery",
                "tools": ["search_products"],
                "messages": [],
            },
        ]

        summary = build_shopai_crm_summary(conversations=conversations, customers_df=customers_df)

        self.assertEqual(summary["kpis"]["conversations"], 2)
        self.assertEqual(summary["kpis"]["needs_attention"], 1)
        self.assertEqual(summary["kpis"]["linked_customers"], 1)
        self.assertAlmostEqual(summary["kpis"]["resolution_rate"], 50.0)
        self.assertTrue(any("VIP" in recommendation for recommendation in summary["recommendations"]))


if __name__ == "__main__":
    unittest.main()
