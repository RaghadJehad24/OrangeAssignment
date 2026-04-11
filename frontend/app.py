import streamlit as st
import requests
import os

st.set_page_config(page_title="Data Insights Bot", page_icon="🤖", layout="centered")

st.title("🛍️ Retail Sales Assistant")
st.markdown("Ask your data warehouse anything in **English** or **Arabic**.")

API_URL = os.getenv("API_URL", "http://backend:8000")

def call_backend(question):
    return requests.post(f"{API_URL}/ask", json={"question": question})

if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if message.get("sql"):
            with st.expander("🔍 View Database Query (For Debugging)"):
                st.code(message["sql"], language="sql")

if prompt := st.chat_input("E.g., What is the average order value?"):

    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    try:
        with st.spinner("Searching the Data ..."):
            response = call_backend(prompt)

            if response.status_code == 200:
                data = response.json()

                bot_message = data.get("message", "عذراً، لم أتمكن من استخراج الرد.")
                sql_used = data.get("sql") 

                with st.chat_message("assistant"):
                    st.markdown(bot_message)
                    
                    if sql_used:
                        with st.expander("🔍 View Database Query (For Debugging)"):
                            st.code(sql_used, language="sql")

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": bot_message,
                    "sql": sql_used
                })

            else:
                try:
                    error_detail = response.json().get("detail", response.text)
                except:
                    error_detail = response.text

                st.error(f"Backend Error ({response.status_code}): {error_detail}")

    except Exception as e:
        st.error(f"Connection failed: {e}")