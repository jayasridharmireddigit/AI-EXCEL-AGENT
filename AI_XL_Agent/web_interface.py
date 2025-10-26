import streamlit as st
import pandas as pd
import sqlite3

# Set page config
st.set_page_config(page_title="Batch File Processor", layout="centered")

# Title
st.title("AI Excel Agent")

# File uploader
uploaded_file = st.file_uploader("Upload your batch file", type=["csv", "xlsx"])



# conn = sqlite3.connect("workbook.db")

# # Write each sheet to DB
# for sheet_name, df in sheets.items():
#     df.to_sql(sheet_name, conn, if_exists="replace", index=False)

# Text input (chat-style)
user_text = st.chat_input("Enter a description or keyword")

# Process file if uploaded
if uploaded_file:
    st.success("File uploaded successfully!")

    sheets = pd.read_excel(uploaded_file, sheet_name=None)
    
    conn = sqlite3.connect("excel_to_sql_data.db")

    # Write each sheet to DB
    for sheet_name, df in sheets.items():
        df.to_sql(sheet_name, conn, if_exists="replace", index=False)
    
    # Read file based on extension
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    elif uploaded_file.name.endswith(".xlsx"):
        df = pd.read_excel(uploaded_file)
    else:
        st.error("Unsupported file format.")
        df = None

    # Display file preview
    if df is not None:
        st.subheader("File Preview")
        st.dataframe(df.head())

        # Use the text input for filtering or tagging
        if user_text:
            st.markdown(f"You entered: **{user_text}**")

            # Filter rows containing the keyword
            filtered_df = df[df.apply(
                lambda row: row.astype(str).str.contains(user_text, case=False).any(),
                axis=1
            )]

            st.subheader("Filtered Data")
            st.dataframe(filtered_df)
else:
    st.info("Please upload a file to begin.")
