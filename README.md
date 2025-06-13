# Globalfaces Donor Intelligence Dashboard

This mobile-friendly Streamlit app connects to Snowflake and delivers real-time donor insights with a responsive UI for both desktop and phone users.

---

## ✅ Features

- Real-time donor data from Snowflake via Snowpark
- Responsive layout for mobile/desktop using `st.columns` + dynamic JS width detection
- Clean secrets management with `secrets.toml`
- Ready for deployment on Streamlit Cloud or local testing

---

## 📦 Requirements

Python 3.9+ and the following packages (included in `requirements.txt`):

- streamlit
- snowflake-snowpark-python
- pandas
- altair
- pytz

---

## 🔐 Setup: Secrets Configuration

Create `.streamlit/secrets.toml`:

```toml
[snowflake]
account = "your_account"           # e.g. xyz12345.ca-central-1
user = "your_username"
password = "your_password"
role = "your_role"
warehouse = "your_warehouse"
database = "DEMO_DB"
schema = "PUBLIC"
```

---

## 💻 Local Deployment (Windows-friendly)

1. Open Command Prompt or PowerShell
2. Navigate to your project folder
3. Run:

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

---

## ☁️ Streamlit Cloud Deployment

1. Push your folder to a **public GitHub repository**
2. Go to [https://streamlit.io/cloud](https://streamlit.io/cloud)
3. Create a new app using `streamlit_app.py` as the entry point
4. Paste your `.streamlit/secrets.toml` contents into **Secrets Manager**
5. Click **Deploy**

---

## 📝 Notes

- This app uses `st.query_params` and injected JS to adjust layout based on screen size.
- Ideal for dashboards displayed in mobile, tablet, or TV formats.
- All secrets are externalized — do not hardcode credentials in Python.

---
