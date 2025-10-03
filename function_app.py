import logging
import datetime
import requests
import smartsheet
import pyodbc
import json
from azure.storage.blob import BlobServiceClient
from azure.communication.email import EmailClient
from azure.core.credentials import AzureKeyCredential
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, NoEncryption
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates
from cryptography.hazmat.backends import default_backend
import tempfile
import azure.functions as func
import re


#prod urls
RR_URL = "url"
pfx_path = "file.pfx"
pfx_password = b"2024"
MS1_URL = "https://ResourceRequest"
MS2_URL = "https://ResourceSkillRequest"
MS3_URL = "https://Status_Read"

DB_CONNECTION_STRING = "DB string"
BLOB_CONN_STR = "connection string"
BLOB_CONTAINER = "container Name"
EMAIL_CONN_STR = "communication string"
EMAIL_SENDER = "mail"
EMAIL_RECIPIENT = "mail.com"
destServer = 'db,1433'
destUser = 'admin'
destDatabase = 'sqldb'
password = 'may25'
SMARTSHEET_TOKEN = "Czht"
SMARTSHEET_SHEET_ID = '021636'
SMARTSHEET_STATUS_COLUMN_ID = '2660'


# ----------------------------------
# FUNCTION BLOCKS
# ----------------------------------

def fetch_sheet():
    client = smartsheet.Smartsheet(SMARTSHEET_TOKEN)
    return client.Sheets.get_sheet(SMARTSHEET_SHEET_ID), client


def get_cert_files_from_pfx(pfx_path: str, pfx_password: bytes) -> tuple[str, str]:
    """
    Extracts certificate and private key from .pfx using `cryptography` and writes to temp files.

    Args:
        pfx_path (str): Path to the .pfx file.
        pfx_password (bytes): Password for the .pfx file, already in bytes format.
        
    Returns:
        tuple[str, str]: Tuple containing paths to the temp cert (.pem) and key (.key) files.
    """
    with open(pfx_path, "rb") as f:
        pfx_data = f.read()

    private_key, certificate, _ = load_key_and_certificates(
        pfx_data, 
        pfx_password,  # Don't call encode() here
        backend=default_backend()
    )

    # Serialize cert and key
    cert_bytes = certificate.public_bytes(Encoding.PEM)
    key_bytes = private_key.private_bytes(
        Encoding.PEM,
        PrivateFormat.PKCS8,
        NoEncryption()
    )

    # Write to temp files
    cert_file = tempfile.NamedTemporaryFile(delete=False, suffix=".pem")
    key_file = tempfile.NamedTemporaryFile(delete=False, suffix=".key")

    cert_file.write(cert_bytes)
    cert_file.flush()
    cert_file.close()

    key_file.write(key_bytes)
    key_file.flush()
    key_file.close()

    return cert_file.name, key_file.name


def get_owner_id(email):
    response = requests.get(f"https://urlEmail%20=%27{email}%27", headers={'Authorization': 'Basic auth='}, verify=False)
    if "error" in response.text.lower():
        raise Exception(f"Owner ID fetch failed: {response.text}")
    return json.loads(response.text)["response"]["records"][0]["Id"],json.loads(response.text)["response"]["records"][0]["Name"]

def get_unity_id(oppId):
    response = requests.get(f"https://c=%27{oppId}%27", headers={'Authorization': 'Basic ='}, verify=False)
    if "error" in response.text.lower():
        raise Exception(f"Owner ID fetch failed: {response.text}")
    return json.loads(response.text)["response"]["records"][0]["Id"],response.json()["response"]["records"][0]['Country__c']

def create_resource_request(owner_id,email,hours,start,end,delivery_location,role_request,delivery_method,practice_name,r_id,note,role):
    
    match = re.search(
    r'^(.*?)\s+-\s+(OPE-\d+)\s+-\s+GEO:\s+(\w+)',
    role_request,
    re.IGNORECASE
    )
    customer_name = match.group(1).strip()
    oppId_extracted = match.group(2).strip()
    geo = match.group(3).capitalize()
    print(f"Oppid: {oppId_extracted}")
    print(f"GEO: {geo}")
    oppId, remote_country = get_unity_id(oppId_extracted)
    practice = get_data_from_database("PracticeData", "Practice_Name", practice_name)
    region = get_data_from_database("CountryMap", "Country", remote_country if delivery_location == 'None' else delivery_location)
    
    print("Pattern not found.")
    payload = json.dumps({
        "pse__Status__c": "Ready to staff",
        "OwnerId": owner_id, #Named Resource Email or owner 
        "pse__Start_Date__c": start,
        "GSD_Region_Name__c": geo,
        "pse__End_Date__c": end,
        "pse__SOW_Hours__c": hours,
        "GSD_Delivery_Method_1__c": "Local",
        "GSD_Task_Type__c": "Pursuit Assistance", 
        "pse__Practice__c": practice[0].get("Pse_Id", "").strip(),
        "pse__Opportunity__c": oppId,
        "pse__Notes__c": f"Requestor email: {email} \n Role: {role}",
        "GSD_Request_For__c": "PSA Resource",
        "PNTSkills_Position_Allocated__c":"Part Time"
    })
    cert_path, key_path = get_cert_files_from_pfx(pfx_path, pfx_password)

    headers = {
        'ClientName': 'PSA',
        'Authorization': 'Basic ==',
        'Content-Type': 'application/json'
    }
    response = requests.post(MS1_URL, headers=headers, data=payload,  cert=(cert_path, key_path), verify=False)
    return{
        "id": response.json()['result']['id'],
        "payload": payload
    }

def assign_skills(rr_id,role):

    payload = json.dumps([
        {
            "pse__skill_Certification__c": "",
            "pse__Resource_Request__c": rr_id,
        }
    ])
    cert_path, key_path = get_cert_files_from_pfx(pfx_path, pfx_password)
    headers = {
        'ClientName': 'PSA',
        'Content-Type': 'application/json',
        'Authorization': 'Basic '
    }
    return requests.post(MS2_URL, headers=headers, data=payload, cert=(cert_path, key_path),verify=False)

def get_resource_name(rr_id):
    cert_path, key_path = get_cert_files_from_pfx(pfx_path, pfx_password)
    payload = json.dumps({
    "Id":{
            "=":f"{rr_id}"
        }
    })
    headers = {
        'ClientName': 'PSA',
        'Content-Type': 'application/json',
        'Authorization': 'Basic '
    }
    return requests.post(MS3_URL, headers=headers, data=payload, cert=(cert_path, key_path),verify=False)

def store_in_database(rr_id,payload,r_id,assign_sks):
    conn = pyodbc.connect(DB_CONNECTION_STRING)
    cursor = conn.cursor()
    cursor.execute(f"""Insert""", r_id,payload,assign_sks,rr_id, datetime.datetime.now())
    conn.commit()
    cursor.close()
    conn.close()

def get_data_from_database(table_name, column_name, column_value):
    try:
        conn = pyodbc.connect(
            f'DRIVER={{SQL Server}};SERVER={destServer};DATABASE={destDatabase};UID={destUser};PWD={password}'
)
        cursor = conn.cursor()

        cursor.execute(f"SELECT * FROM {table_name} where {column_name} = '{column_value}'")
        rows = cursor.fetchall()

        # Convert to list of dicts for easier handling
        columns = [column[0] for column in cursor.description]
        result = [dict(zip(columns, row)) for row in rows]

        cursor.close()
        conn.close()

        return result

    except Exception as e:
        print(f"Error while retrieving data: {e}")
        return []

def update_smartsheet(client, row_id,id):
    update_row = smartsheet.models.Row()
    update_row.id = row_id
    update_row.cells = [{
        'column_id': SMARTSHEET_STATUS_COLUMN_ID,
        'value': id
    }]
    client.Sheets.update_rows(SMARTSHEET_SHEET_ID, [update_row])

def log_to_blob(log_messages):
    blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONN_STR)
    container_client = blob_service_client.get_container_client(BLOB_CONTAINER)
    try:
        container_client.create_container()
    except:
        pass
    blob_name = f"log-{datetime.datetime.now().strftime('%Y%m%d-%H%M%S')}.txt"
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob("\n".join(log_messages), overwrite=True)

def send_email(success, log_messages = "ran successfully"):
    try:
        client = EmailClient.from_connection_string(EMAIL_CONN_STR)
        html_content = f"""
        <html>
        <head>
          <style>
            body {{ font-family: 'Segoe UI', sans-serif; background-color: #f4f4f4; color: #333; padding: 20px; }}
            .container {{ background-color: #fff; padding: 20px; border-radius: 10px; box-shadow: 0px 0px 8px rgba(0,0,0,0.1); }}
            h2 {{ color: {'#28a745' if success else '#dc3545'}; }}
            pre {{ background-color: #f8f9fa; padding: 10px; border-left: 4px solid #007bff; overflow-x: auto; }}
          </style>
        </head>
        <body>
          <div class="container">
            <h2>{'✅ Scheduler Succeeded' if success else '❌ Scheduler Failed'}</h2>
            <p>Here is the log summary for the timer-triggered function execution:</p>
            <pre>{'\n'.join(log_messages)}</pre>
            <p style="font-size: 0.9em; color: #888;">This is an automated email sent by your Azure Function App.</p>
          </div>
        </body>
        </html>
        """
        message = {
    "senderAddress": EMAIL_SENDER,
    "recipients": {
        "to": [
            {"address": "mail.com"}
        ]
    },
    "content": {
        "subject":"Azure Scheduler Report ✔" if success else "Azure Scheduler Failure ❌",
        "html": html_content
    }
}
        poller = client.begin_send(message)
        result = poller.result()
    except Exception as e:
        logging.error(f"Email failed to send: {str(e)}")

def send_email_success(url,rr_name,name,email):
    try:
        client = EmailClient.from_connection_string(EMAIL_CONN_STR)
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
</head>
<body style="margin:0; padding:0; background-color:#f6f9fc;">
  <table role="presentation" cellpadding="0" cellspacing="0" width="100%" bgcolor="#f6f9fc" style="background-color:#f6f9fc; width:100%;">
    <tr>
      <td align="center" style="padding:30px 0;">
        <table role="presentation" cellpadding="0" cellspacing="0" width="600" style="width:600px; background-color:#ffffff;" bgcolor="#ffffff">
          <!-- Header -->
          <tr>
            <td align="center" style="background-color:#01A982; color:#ffffff; padding:20px; font-family:Arial, sans-serif; font-size:24px;">
              Resource Request Created Successfully
            </td>
          </tr>

          <!-- Body -->
          <tr>
            <td style="padding:30px; font-family:Arial, sans-serif; color:#333333; font-size:16px;">
              <p style="margin-top:0;">Hi {name},</p>
              <p>Resource Request is Created Successfully. Please click on the link below to review</p>
              <table role="presentation" cellpadding="0" cellspacing="0" style="margin-top:20px;">
<tr>
  <td align="center" style="padding: 20px 0;">
    <!--[if mso]>
    <v:roundrect xmlns:v="urn:schemas-microsoft-com:vml" xmlns:w="urn:schemas-microsoft-com:office:word" href="{url}" style="height:40px;v-text-anchor:middle;width:160px;" arcsize="10%" strokecolor="#0078D4" fillcolor="#0078D4">
      <w:anchorlock/>
      <center style="color:#ffffff;font-family:Arial,sans-serif;font-size:16px;font-weight:bold;">
        View
      </center>
    </v:roundrect>
    <![endif]-->
    <![if !mso]>
    <a href="{url}" target="_blank" style="background-color:#0078D4;border:1px solid #0078D4;border-radius:4px;color:#ffffff;display:inline-block;font-family:Arial,sans-serif;font-size:16px;font-weight:bold;line-height:40px;text-align:center;text-decoration:none;width:160px;-webkit-text-size-adjust:none;mso-hide:all;">
      View
    </a>
    <![endif]>
  </td>
</tr>
              </table>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
        """
        message = {
    "senderAddress": EMAIL_SENDER,
    "recipients": {
        "to": [
            {"address": email},
            
        ],        
        "cc": [
            {"address": "gmail.com"}
        ]
    },
    "content": {
        "subject": f"Resource Request Created Successfully - {rr_name}",
        "html": html_content
    }
}
        poller = client.begin_send(message)
        result = poller.result()
    except Exception as e:
        logging.error(f"Email failed to send: {str(e)}")

# ----------------------------------
# TIMER FUNCTION
# ----------------------------------
app = func.FunctionApp()

# ┌──────────── second (0)
# │ ┌────────── minute (0)
# │ │ ┌──────── hour (16 = 4 PM)
# │ │ │ ┌────── day of month (* = every day)
# │ │ │ │ ┌──── month (* = every month)
# │ │ │ │ │ ┌── day of week (* = every day)
# │ │ │ │ │ │
#  0  0 16  *  *  *
#@app.timer_trigger(schedule="0 0 16 * * *", arg_name="myTimer", run_on_startup=False, use_monitor=False)

@app.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer",run_on_startup=False,
              use_monitor=False)

def smartsheetAutomation(myTimer: func.TimerRequest) -> None:
    log_messages = []
    success = True
    logging.info('Python timer trigger function executing')
    if myTimer.past_due:
        log_messages.append("Timer is past due!")

    try:
        sheet, client = fetch_sheet()
        for row in sheet.rows:
            try:
                row_id = row.id
                cells = {str(cell.column_id): str(cell.value) for cell in row.cells}
                if cells.get("728608948","").strip() == 'Resource Request':
                    email = cells.get("20780", "").strip()
                    hours = cells.get("65094", "").strip()
                    start = cells.get("7635888", "").strip()
                    end = cells.get("20063298", "").strip()
                    delivery_location = cells.get("31328922", "").strip()
                    role_request = cells.get("36951788", "").strip()
                    delivery_method = cells.get("1685320", "").strip()
                    practice_name = cells.get("1304502596", "").strip()
                    note = cells.get("1651700", "").strip()
                    role = cells.get("88041844","").strip()
                    r_id = cells.get("17635204", "").strip()
                
                    if not email:
                        raise Exception("Email is missing in Smartsheet row.")

                    owner_id,name = get_owner_id(email)
                
                    response = create_resource_request(owner_id,email,hours,start,end,delivery_location,role_request,delivery_method,practice_name,r_id,note,role)
                
                    rr_name = get_resource_name(response["id"])
                    assign_sks = assign_skills(response["id"],role)
                    log_messages.append(f"skill response : {assign_sks} / RR ID :  {response["id"]}.")
                    send_email_success(f"{RR_URL}{response["id"]}/view",json.loads(rr_name.text)['result'][0]['Name'],name,email)
                    store_in_database(response["id"],response["payload"],r_id,assign_sks)
                    update_smartsheet(client, row_id,rr_name.json()['result'][0]['Name'])
                    log_messages.append(f"Row {row_id} processed successfully with RR ID {response["id"]}.")

            except Exception as row_err:
                success = False
                log_messages.append(f"Row {row.id} error: {str(row_err)}")
    except Exception as e:
        success = False
        log_messages.append(f"Fatal error: {str(e)}")

    log_to_blob(log_messages)
    send_email(success, log_messages)
    logging.info('Python timer trigger function executed')

