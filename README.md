# Inventory Distribution (Streamlit)

## Run

```bash
pip install -r requirements.txt
streamlit run app.py
```

## What it does

- Upload **Product List** (xlsx/csv)
- Upload inventory files for locations (**Ecom**, **Mirpur**, **Wari**, **Cumilla**, **Sylhet**)
- Downloads an Excel report with those location columns filled with **stock**

## Notes

- Matching is case-insensitive.
- Numeric Excel artifacts like `123.0` are normalized to `123` so `Ecom/Mirpur/Wari` columns populate correctly.

