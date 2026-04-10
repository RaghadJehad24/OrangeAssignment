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
        if message["role"] == "user":
            st.markdown(message["content"])
        else:
            st.markdown(message["content"])

if prompt := st.chat_input("E.g., What is the average order value?"):

    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.chat_message("user"):
        st.markdown(prompt)

    try:
        with st.spinner("Searching the Data Warehouse..."):
            print("Sending:", prompt)

            response = call_backend(prompt)

            if response.status_code == 200:
                data = response.json()

                answer = data.get("result")
                generated_sql = data.get("sql") 

                with st.chat_message("assistant"):
                    st.markdown("### 📊 Answer")
                    st.write(answer)

                    st.markdown("### 🧠 Generated SQL")
                    st.code(generated_sql, language="sql")

                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"### 📊 Answer\n{answer}\n\n### 🧠 Generated SQL\n```sql\n{generated_sql}\n```"
                })

            else:
                try:
                    error_detail = response.json().get("detail", response.text)
                except:
                    error_detail = response.text

                st.error(f"Backend Error ({response.status_code}): {error_detail}")

    except Exception as e:
        st.error(f"Connection failed: {e}")