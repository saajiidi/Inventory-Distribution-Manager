# 📊 DEEN Business Intelligence
### **AI-Powered Predictive Operations Intelligence System**

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://deen-business-intel.streamlit.app/)
[![Code Quality](https://img.shields.io/badge/Code%20Quality-Senior%2B-blueviolet)](https://github.com/saajiidi/DEEN-AI-Dashboard)

"Most dashboards visualize. This terminal explains, predicts, and recommends."

---

## 🔴 The Problem
Standard BI tools provide charts but lack **contextual intelligence**. Management is often left asking: *"Why did sales drop?"*, *"Which products should we bundle?"*, or *"When will we run out of stock for our top kits?"*

## ✅ The Solution: DEEN OPS Terminal
A professional-grade **Operational Command Center** designed for high-velocity E-commerce. It transforms raw WooCommerce data into an actionable decision-support system using multi-tier machine learning.

---

## 🧠 Core Intelligence Pillars

### 1. Enterprise-Grade AutoML Forecasting
Unlike simple linear trends, our **Smart Model Router** automatically evaluates the dataset's characteristics (stationarity, seasonality, sparsity) to select the optimal model from our tournament:
*   **Tier 1 (Statistical):** Exponential Smoothing (Holt-Winters), Ridge/LASSO Regression.
*   **Tier 2 (Classical):** SARIMA, **Croston's Method** (for intermittent/sparse demand).
*   **Tier 3 (Supervised ML):** XGBoost, LightGBM with rolling feature engineering.
*   **Tier 4 (Deep Learning):** Prophet and LSTM for complex long-term dependencies.

### 2. Market Basket & Affinity Analysis (MBA)
Discover hidden revenue opportunities using association rule learning (Apriori).
*   **Support/Confidence/Lift Metrics:** Identify which products are "better together".
*   **Attachment Rate tracking:** Monitor the performance of strategic product pairings.

### 3. Bundle-Aware Inventory Intelligence
The system joins real-time stock levels with sales affinity data to prevent "Orphan Stock":
*   **Bundle Fulfillment Rate:** Identifies the "bottleneck component" in popular kits.
*   **Orphan Stock Rate:** Detects capital trapped in accessories whose core product is OOS.
*   **Strategic Reorder Alerts:** Recommends joint purchases based on component dependency.

### 4. High-Fidelity Data Pipeline
*   **Auto-Fetch Engine:** Seamless background synchronization with external APIs.
*   **Dynamic Column Mapping:** Flexible ingestion logic that handles schema drift automatically.
*   **Anomaly Toasting:** Real-time UI notifications for unusual refund spikes or stockouts.

---

## 🛠️ Technology Stack
*   **Frontend:** Streamlit (Custom Material-Design CSS Override), Plotly.
*   **Backend:** Service-Oriented Architecture (SOA), Python 3.x.
*   **Machine Learning:** Scikit-Learn, Statsmodels, XGBoost, Prophet.
*   **Data Ops:** Pandas (Vectorized Ops), NumPy, Aiohttp (Async Syncing).

---

## 📸 System Walkthrough

*Coming Soon... (Insert High-Quality GIF here)*

---

## 🚀 Getting Started

1. **Clone & Install:**
   ```bash
   git clone https://github.com/saajiidi/DEEN-AI-Dashboard.git
   pip install -r requirements.txt
   ```
2. **Configure Secrets:**
   Add your `[woocommerce]` and `[auth]` credentials to `.streamlit/secrets.toml`.
3. **Run Terminal:**
   ```bash
   streamlit run app.py
   ```

---
**Engineered with precision for DEEN Commerce.**
*Primary Developer: Sajid Islam*
