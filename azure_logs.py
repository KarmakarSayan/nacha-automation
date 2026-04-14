import smtplib
from email.message import EmailMessage
from azure.identity import ClientSecretCredential
from azure.monitor.query import LogsQueryClient
from datetime import datetime, timedelta, timezone
import pandas as pd
import os

# ----------- AZURE CONFIG -----------
resource_id = "/subscriptions/e55e5916-f6a5-42b6-b4e4-8e5de225fba0/resourceGroups/rg-nacha-csp-prod/providers/Microsoft.Insights/components/appi-nacha-csp-prod"

tenant_id = os.getenv("TENANT_ID")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

# ----------- SMTP CONFIG -----------
sender_email = os.getenv("SMTP_EMAIL")
password = os.getenv("SMTP_PASSWORD")
receiver_email = os.getenv("RECEIVER_EMAIL")

# ----------- AUTHENTICATION -----------
credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

client = LogsQueryClient(credential)

# ----------- TIME RANGE (LAST 24 HOURS UTC) -----------
end_utc = datetime.now(timezone.utc)
start_utc = end_utc - timedelta(hours=24)

start_utc_str = start_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
end_utc_str = end_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

print(f"\nUTC Window: {start_utc} → {end_utc}")

# ----------- QUERY 1 (FAILURES) -----------
query_1 = f"""
dependencies
| where timestamp between (datetime({start_utc_str}) .. datetime({end_utc_str}))
| where name == "POST /ACHCheckPrescreen/GetReport"
| where success != true
| project name, appId, target, success, resultCode, ["TimeStamp(UTC)"] = timestamp
"""

# ----------- QUERY 2 (SUMMARY) -----------
query_2 = f"""
dependencies
| where timestamp between (datetime({start_utc_str}) .. datetime({end_utc_str}))
| where name == "POST /ACHCheckPrescreen/GetReport"
| summarize 
    SuccessCount = countif(success == true),
    FailureCount = countif(success == false),
    TotalCount = count()
| extend SuccessRate = round((SuccessCount * 100.0) / TotalCount, 2)
"""

# ----------- RESPONSE TO DATAFRAME -----------
def response_to_df(response):
    for table in response.tables:
        columns = [col.name if hasattr(col, "name") else col for col in table.columns]
        rows = table.rows
        return pd.DataFrame(rows, columns=columns)
    return pd.DataFrame()

# ----------- REMOVE TIMEZONE -----------
def remove_timezone(df):
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            try:
                df[col] = df[col].dt.tz_localize(None)
            except:
                df[col] = df[col].apply(
                    lambda x: x.replace(tzinfo=None) if pd.notnull(x) else x
                )
    return df

# ----------- RUN QUERIES -----------
print("\nRunning Query 1...")
response1 = client.query_resource(resource_id, query_1, timespan=(start_utc, end_utc))
df1 = response_to_df(response1)

print("\nRunning Query 2...")
response2 = client.query_resource(resource_id, query_2, timespan=(start_utc, end_utc))
df2 = response_to_df(response2)

# ----------- CLEAN DATA -----------
df1 = remove_timezone(df1)
df2 = remove_timezone(df2)

# ----------- SAVE EXCEL -----------
folder_path = "Nacha_Daily_reports"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

today_date = datetime.now().strftime("%Y-%m-%d")
file_path = os.path.join(folder_path, f"Nacha-{today_date}.xlsx")

counter = 1
while os.path.exists(file_path):
    file_path = os.path.join(folder_path, f"Nacha-{today_date}_{counter}.xlsx")
    counter += 1

with pd.ExcelWriter(file_path, engine="openpyxl") as writer:
    df1.to_excel(writer, sheet_name="Failures", index=False)
    df2.to_excel(writer, sheet_name="Summary", index=False)

print(f"\n Excel file generated: {file_path}")

# ----------- SEND EMAIL (SMTP) -----------

msg = EmailMessage()
msg["Subject"] = f"NACHA Daily Report - {today_date}"
msg["From"] = sender_email
msg["To"] = receiver_email

msg.set_content(
    f"""Hi Team,

Please find attached the NACHA daily report for {today_date} UTC .

Regards,
Sayan Karmakar"""
)

# Attach Excel file
with open(file_path, "rb") as f:
    msg.add_attachment(
        f.read(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=os.path.basename(file_path)
    )

# Send email
try:
    with smtplib.SMTP("smtp.office365.com", 587) as server:
        server.starttls()
        server.login(sender_email, password)
        server.send_message(msg)

    print(" Email sent successfully!")

except Exception as e:
    print(" Email failed:", str(e))