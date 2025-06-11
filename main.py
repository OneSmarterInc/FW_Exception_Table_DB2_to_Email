import os
import pandas as pd
import pyodbc
import requests
from dotenv import load_dotenv

# DB2 Connection
def get_connection():
    print("🔌 Connecting to AS400 DB2 database...")

    connection_string = (
        f"DRIVER={{iSeries Access ODBC Driver}};"
        f"SYSTEM=104.153.122.227;"
        f"PORT=23;"
        f"DATABASE=S78F13CW;"
        f"UID=ONEPYTHON;"
        f"PWD=pa33word;"
        f"PROTOCOL=TCPIP;"
    )
    
    try:
        conn = pyodbc.connect(connection_string)
        print("✅ Database connection successful.")
        return conn
    except Exception as e:
        print("❌ Database connection failed:", e)
        raise

# Fetch data from DB
def fetch_data(query):
    print(f"📄 Running query: {query}")
    try:
        conn = get_connection()
        df = pd.read_sql(query, conn)
        conn.close()
        print(f"✅ Query successful. Retrieved {len(df)} rows.")
        return df
    except Exception as e:
        print("❌ Error fetching data:", e)
        raise

# Email body with styled HTML table
def build_email_body(table_html):
    return f"""
    <html>
    <head>
        <style>
            .data-table {{
                width: 80%;
                border-collapse: collapse;
                font-family: Arial, sans-serif;
                margin-top: 10px;
                border: 1px solid #999;
            }}
            .data-table th {{
                background-color: #19BAFF;
                color: black;
                text-align: left;
                padding: 8px;
                border: 1px solid #999;
            }}
            .data-table td {{
                padding: 8px;
                text-align: left;
                border: 1px solid #999;
            }}
            .data-table tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
        </style>
    </head>
    <body>
        <p>Kindly find the attached report below:</p>
        {table_html}
 
    </body>
    </html>
    """


# Send email using API via form-data (multipart)
def send_email_via_api(email, subject, html_body):
    print(f"📤 Sending email to {email} via API with subject: {subject}")
    try:
        # Send as form-data instead of JSON
        payload = {
            "email": email,
            "subject": subject,
            "body": html_body
        }
        response = requests.post("http://104.153.122.230:8127/send-email", data=payload)

        if response.status_code == 200:
            print("✅ Email sent successfully via API.\n")
        else:
            print(f"❌ API error {response.status_code}: {response.text}")
    except Exception as e:
        print("❌ Failed to send email via API:", e)
        raise

# Main process
if __name__ == "__main__":
    print("🚀 Starting DB2 email report process...\n")
    try:
        # === Table 1 ===
        df1 = fetch_data("SELECT * FROM gnglib.hcidblnk")
        df1 = df1.rename(columns={"IDTYPE": "ID", "FUNDLIB" :"FundLib", "EMSSN": "SSN", "EMNAME": "Name", "EHHLTH" : "Health Elg Status" , "ELGDATE": "Eligibility Date"})
        # Select only the renamed columns 
        df1 = df1[["ID", "FundLib", "SSN", "Name", "Health Elg Status", "Eligibility Date"]]
        html1 = build_email_body(df1.to_html(index=False, classes="data-table", border=0))
        receivers1 = ["gearbold@gmail.com"]
        subject1 = "FW: Exception Alert - HCID/Highmark ID Blank Cases"
        for email in receivers1:
            send_email_via_api(email.strip(), subject1, html1)

        # === Table 2 ===
        df2 = fetch_data("SELECT * FROM gnglib.ALRTS01")
        df2 = df2.rename(columns={"FUNDLIB": "FundLib", "EMSSN": "SSN", "EMNAME": "Name", "XHCID": "HCID"})
        html2 = build_email_body(df2.to_html(index=False, classes="data-table", border=0))
        receivers2 = ["prajvalghusalikar@gmail.com"]
        subject2 = "FW: Exception Alert - HCID Duplicate"
        for email in receivers2:
            send_email_via_api(email.strip(), subject2, html2)

        print("\n🏁 All emails sent successfully.")
    except Exception as e:
        print("🚨 Process terminated due to error:", e)
