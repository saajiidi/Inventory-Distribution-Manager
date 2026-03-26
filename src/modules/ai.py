import streamlit as st


def render_ai_chat_tab():
    try:
        import google.generativeai as genai
        import openai
    except ImportError:
        st.warning(
            "🤖 AI modules not fully installed. Run `pip install google-generativeai openai` to enable full chat."
        )
        return

    st.subheader("🤖 AI Data Analyst")

    # API Selection
    col1, col2 = st.columns(2)
    with col1:
        provider = st.selectbox(
            "AI Provider", ["Google Gemini (Recommended)", "OpenAI"]
        )
    with col2:
        api_key = st.text_input("Enter API Key", type="password")

    if not api_key:
        st.info("Please enter an API key to start chatting about your data.")
        return

    # Unified chat logic
    if "messages" not in st.session_state:
        st.session_state.messages = []

    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    if prompt := st.chat_input("Ask about your orders, sales trends, or stock..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            try:
                # Prepare data context
                context = ""
                if (
                    "wp_preview_df" in st.session_state
                    and st.session_state.wp_preview_df is not None
                ):
                    summary = st.session_state.wp_preview_df.head(10).to_string()
                    context = f"\n\nData Context (Preview):\n{summary}"

                full_prompt = f"{prompt}{context}"

                if provider == "Google Gemini (Recommended)":
                    genai.configure(api_key=api_key)
                    model = genai.GenerativeModel("gemini-1.5-flash")
                    response = model.generate_content(full_prompt)
                    answer = response.text
                else:
                    client = openai.OpenAI(api_key=api_key)
                    response = client.chat.completions.create(
                        model="gpt-3.5-turbo",
                        messages=[{"role": "user", "content": full_prompt}],
                    )
                    answer = response.choices[0].message.content

                st.markdown(answer)
                st.session_state.messages.append(
                    {"role": "assistant", "content": answer}
                )
            except Exception as e:
                st.error(f"AI Error: {str(e)}")
