### SYSTEM ERROR DETECTED FOR FIXING

Context: Returns Background Load
Error Type: KeyError
Error: 'BackEnd.core.paths'
Timestamp: 2026-04-20 16:50:32

Environment:
```json
{
  "python": "3.14.4",
  "platform": "Windows-11-10.0.26200-SP0",
  "cwd": "H:\\Repo\\DEEN-BI"
}
```

Additional Details:
```json
{}
```

Traceback:
```python
Traceback (most recent call last):
  File "H:\Repo\DEEN-BI\FrontEnd\pages\dashboard.py", line 390, in _load_returns_async
    returns_df = load_returns_data(sync_window=window, sales_df=sales_df)
  File "C:\Users\deenb\AppData\Roaming\Python\Python314\site-packages\streamlit\runtime\caching\cache_utils.py", line 281, in __call__
    return self._get_or_create_cached_value(args, kwargs, spinner_message)
           ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\deenb\AppData\Roaming\Python\Python314\site-packages\streamlit\runtime\caching\cache_utils.py", line 326, in _get_or_create_cached_value
    return self._handle_cache_miss(cache, value_key, func_args, func_kwargs)
           ~~~~~~~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "C:\Users\deenb\AppData\Roaming\Python\Python314\site-packages\streamlit\runtime\caching\cache_utils.py", line 385, in _handle_cache_miss
    computed_value = self._info.func(*func_args, **func_kwargs)
  File "H:\Repo\DEEN-BI\BackEnd\services\returns_tracker.py", line 187, in load_returns_data
    df = cross_reference_return_items(df, sales_df)
  File "H:\Repo\DEEN-BI\BackEnd\services\returns_tracker.py", line 331, in cross_reference_return_items
    order_data = fetch_woocommerce_order_by_id(str(order_id))
  File "H:\Repo\DEEN-BI\BackEnd\services\returns_tracker.py", line 203, in fetch_woocommerce_order_by_id
    from BackEnd.services.woocommerce_service import WooCommerceService
  File "H:\Repo\DEEN-BI\BackEnd\services\__init__.py", line 6, in <module>
    from .customer_insights import (
    ...<6 lines>...
    )
  File "H:\Repo\DEEN-BI\BackEnd\services\customer_insights.py", line 15, in <module>
    from BackEnd.services.customer_manager import load_customer_mapping
  File "H:\Repo\DEEN-BI\BackEnd\services\customer_manager.py", line 13, in <module>
    from BackEnd.core.logging_config import get_logger
  File "H:\Repo\DEEN-BI\BackEnd\core\logging_config.py", line 15, in <module>
    from BackEnd.core.paths import LOGS_DIR
  File "<frozen importlib._bootstrap>", line 1371, in _find_and_load
  File "<frozen importlib._bootstrap>", line 1342, in _find_and_load_unlocked
  File "<frozen importlib._bootstrap>", line 949, in _load_unlocked
KeyError: 'BackEnd.core.paths'

```

Task:
1. Explain the likely root cause.
2. Identify the safest code change.
3. Suggest tests to prevent regression.
4. Mention any schema mismatch, missing secret, or data-quality issue involved.
