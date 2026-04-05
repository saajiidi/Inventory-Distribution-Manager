from unittest import TestCase
from unittest.mock import patch

from BackEnd.services.woocommerce_service import get_woocommerce_credentials


class WooCommerceSecretsTests(TestCase):
    def test_returns_credentials_when_required_keys_exist(self):
        fake_secrets = {
            "woocommerce": {
                "store_url": "https://example.com",
                "consumer_key": "ck_test",
                "consumer_secret": "cs_test",
            }
        }

        with patch("BackEnd.services.woocommerce_service.st.secrets", fake_secrets):
            creds = get_woocommerce_credentials()

        self.assertEqual(
            creds,
            {
                "store_url": "https://example.com",
                "consumer_key": "ck_test",
                "consumer_secret": "cs_test",
            },
        )

    def test_returns_empty_dict_when_secrets_access_raises(self):
        class BrokenSecrets:
            def get(self, *_args, **_kwargs):
                raise RuntimeError("secrets unavailable")

        with patch("BackEnd.services.woocommerce_service.st.secrets", BrokenSecrets()):
            creds = get_woocommerce_credentials()

        self.assertEqual(creds, {})
