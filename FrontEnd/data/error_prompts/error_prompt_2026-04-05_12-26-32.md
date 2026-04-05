### SYSTEM ERROR DETECTED FOR FIXING

Context: App Bootstrap
Error Type: ValueError
Error: cannot insert order_date, already exists
Timestamp: 2026-04-05 12:26:32

Environment:
```json
{
  "python": "3.14.3",
  "platform": "Windows-11-10.0.26200-SP0",
  "cwd": "H:\\Analysis\\Automation-Pivot"
}
```

Additional Details:
```json
{}
```

Traceback:
```python
Traceback (most recent call last):
  File "H:\Analysis\Automation-Pivot\app.py", line 146, in <module>
    run_app()
    ~~~~~~~^^
  File "H:\Analysis\Automation-Pivot\app.py", line 121, in run_app
    render_dashboard_tab()
    ~~~~~~~~~~~~~~~~~~~~^^
  File "H:\Analysis\Automation-Pivot\FrontEnd\pages\dashboard.py", line 89, in render_dashboard_tab
    render_sales_trends(df_sales)
    ~~~~~~~~~~~~~~~~~~~^^^^^^^^^^
  File "H:\Analysis\Automation-Pivot\FrontEnd\pages\dashboard.py", line 187, in render_sales_trends
    heat = df.groupby([df["order_date"].dt.dayofweek, df["order_date"].dt.hour]).size().reset_index(name="Orders")
  File "C:\Users\deenb\AppData\Roaming\Python\Python314\site-packages\pandas\core\series.py", line 1782, in reset_index
    return df.reset_index(
           ~~~~~~~~~~~~~~^
        level=level, drop=drop, allow_duplicates=allow_duplicates
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\deenb\AppData\Roaming\Python\Python314\site-packages\pandas\core\frame.py", line 6494, in reset_index
    new_obj.insert(
    ~~~~~~~~~~~~~~^
        0,
        ^^
    ...<2 lines>...
        allow_duplicates=allow_duplicates,
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    )
    ^
  File "C:\Users\deenb\AppData\Roaming\Python\Python314\site-packages\pandas\core\frame.py", line 5180, in insert
    raise ValueError(f"cannot insert {column}, already exists")
ValueError: cannot insert order_date, already exists

```

Task:
1. Explain the likely root cause.
2. Identify the safest code change.
3. Suggest tests to prevent regression.
4. Mention any schema mismatch, missing secret, or data-quality issue involved.
