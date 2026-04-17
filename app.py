from __future__ import annotations

import streamlit as st


st.set_page_config(
    page_title="Global Cross-Asset Dashboard",
    page_icon="CA",
    layout="wide",
)


navigation = st.navigation(
    [
        st.Page("pages/01_Overview.py", title="Overview", icon=":material/language:"),
        st.Page("pages/02_Equities.py", title="Equities", icon=":material/show_chart:"),
        st.Page("pages/03_Bonds.py", title="Bonds", icon=":material/account_balance:"),
        st.Page("pages/04_Commodities.py", title="Commodities", icon=":material/oil_barrel:"),
        st.Page("pages/07_ETFs.py", title="ETFs", icon=":material/account_balance_wallet:"),
        st.Page("pages/08_Crypto.py", title="Crypto", icon=":material/currency_bitcoin:"),
        st.Page("pages/09_Top_10_Stocks.py", title="Top-10 Stocks", icon=":material/trending_up:"),
        st.Page("pages/05_Compare.py", title="Compare", icon=":material/grid_view:"),
        st.Page("pages/06_Data_Quality.py", title="Data Quality", icon=":material/verified:"),
    ]
)

navigation.run()

