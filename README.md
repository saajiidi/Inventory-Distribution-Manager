# DEEN Commerce BI - AI Dashboard v2.5.0

A premium, high-performance Business Intelligence dashboard for DEEN Commerce, fully integrated with WooCommerce.

## 🚀 Key Features

- **Business Intelligence**: Real-time sales analytics, AOV tracking, and executive KPIs.
- **Customer Intelligence**: Advanced RFM segmentation, Average Customer Lifetime Value (CLV), and historical purchase-cycle behavioral tracking.
- **Operations Hub**: Unified logistics tools including Pathao bulk processing and WhatsApp verification.
- **Business Cycles**: Operating performance tracking with automated 5:30 PM Bangladesh time-cycle cutoffs.
- **ShopAI CRM**: Intelligent customer support analytics and an AI-driven agent lab.
- **ML Predictive Ensembles**: 7-day automated rolling forecasting overlaying ARIMA, SARIMA, Holt-Winters Exponential Smoothing, Linear Regression, Random Forest, and Naive baselines to automatically calculate the "Best Evaluated Fit".
- **Deep-Dive Clusters**: Dynamic multi-tier cascading dropdowns (Category -> SKU -> Color -> Size) to dissect revenue streams without runtime exceptions.
- **Full Historical Sync**: Seamless background asynchronous syncing allowing custom deep-date range querying directly spanning back to 2022.

## 🏗️ Restructured Architecture

The project has been optimized into a clean, modular structure:
- `BackEnd/`: Service-oriented architecture for data loading, WooCommerce sync, and ML.
- `FrontEnd/`: Component-based UI logic with a modular library for dashboard rendering.
- `BackEnd/commerce_ops/`: Centralized logistics and operational logic.
- `BackEnd/ai_engine/`: NLP query engine for ShopAI.

## 🛠️ Installation

1. Clone the repository.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Set up your `.streamlit/secrets.toml` with WooCommerce API credentials:
   ```toml
   [woocommerce]
   url = "https://yoursearch.com"
   consumer_key = "ck_..."
   consumer_secret = "cs_..."
   ```
4. Run the application:
   ```bash
   streamlit run app.py
   ```

## 💅 Aesthetics

Designed with a **Premium Glassmorphism** aesthetic that supports both System Light and Dark modes.
- **Inter Font Family** for high readability.
- **Minimalist Layout** to reduce noise and focus on actionable metrics.
- **Responsive design** optimized for Desktop, Tablet, and Mobile.

---
© 2026 DEEN Commerce. Engineered by Sajid Islam.
