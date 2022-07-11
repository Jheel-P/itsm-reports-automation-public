# import modules
import os
import gspread
import datetime
import pandas as pd
from google.auth import default
from google.auth.transport.requests import Request
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from google.cloud import bigquery

# set module variables
project_id = os.environ.get("BQ_DATA_PROJECT")
bq_job_project_id = os.environ.get("BQ_USER_PROJECT")
gsheet_url = os.environ.get("GOOGLE_SHEET_URL")
bq_dataset = "servicenow"

query_01 = """
SELECT b.task_display_value Incident_Number, b.sla_target SLA_Target, b.stage_value Stage, b.has_breached_display_value Has_Breached, 
    b.start_time_value Start_Time, b.end_time_value End_Time, b.duration_value Consumed_Time
FROM 
    `searce-cms-production.servicenow.incident` a,
    `searce-cms-production.servicenow.task_sla` b
WHERE
    a.number_display_value = b.task_display_value AND a.assignment_group_display_value != "Workspace Support" AND b.stage_value != "cancelled"
"""
query_02 = """
SELECT a.sys_created_on_value Created, a.priority_display_value Priority, "" Vendor_Case, a.number_display_value Incident_Number, 
    a.short_description_display_value Short_Description, a.assignment_group_display_value Assignment_Group, 
    a.assigned_to_display_value Assignee, a.state_display_value State, a.caller_id_display_value Tool, a.cmdb_ci_display_value Config_Item, 
    b.name_display_value Account, b.u_geography_display_value Geography,
    b.country_display_value Country, a.category_display_value Category, a.subcategory_display_value Sub_Category,
    a.sys_updated_on_value Updated, a.sys_updated_by_display_value Updated_By, a.sys_mod_count_value Updates_Count,
    a.resolved_at_value Resolved, a.close_code_display_value Resolution_Code, a.closed_at_value Closed, 0 Time_Worked, 
    a.u_acknowledgement_time_value Acknowledge_Time, a.reassignment_count_value Reassignment_Count
FROM 
    `searce-cms-production.servicenow.incident` a
LEFT JOIN
    `searce-cms-production.servicenow.cmdb_ci` c ON c.sys_id_value = a.cmdb_ci_value
LEFT JOIN
    `searce-cms-production.servicenow.customer_account` b ON b.sys_id_value = a.u_account_name_value OR b.sys_id_value = c.u_customer_value
WHERE
    a.assignment_group_display_value != "Workspace Support" AND a.parent_incident_display_value IS NULL
    AND (a.sys_created_on_value >= TIMESTAMP_SUB(CURRENT_TIMESTAMP() , INTERVAL 90 DAY) 
    OR a.resolved_at_value >= TIMESTAMP_SUB(CURRENT_TIMESTAMP() , INTERVAL 90 DAY) OR a.resolved_at_value IS NULL)
"""
query_03 = """
SELECT b.number_value Survey_Instance, b.task_id_display_value Incident_Number, b.sys_created_on_value Triggered_On,
    b.taken_on_value Taken_On, a.user_display_value Taken_By, b.percent_answered_value Completed, a.actual_value_value Overall_Rating
FROM 
    `searce-cms-production.servicenow.asmt_metric_result` a,
    `searce-cms-production.servicenow.asmt_assessment_instance` b
WHERE 
    a.instance_display_value = b.number_display_value AND a.actual_value_value != -1
"""

# initialize credentials and generate oauth tokens
credentials, project = default(
    scopes=[
        "https://www.googleapis.com/auth/cloud-platform",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
)
credentials.refresh(Request())
oauth_token = credentials.token
header = {"Authorization": f"Bearer {oauth_token}"}

# initialize gsheet
gspread_client = gspread.authorize(credentials)
gsheet = gspread_client.open_by_url(gsheet_url)


def loadIncidentData():
    # clear incidentdata worksheet
    ws = gsheet.worksheet("IncidentData")
    del_rows = ws.row_count
    try:
        ws.delete_rows(3, del_rows + 1)
    except:
        pass
    res = gsheet.values_clear("'IncidentData'!A2:X2")

    # fetch incident data
    query = query_02
    client = bigquery.Client(project=bq_job_project_id)
    query_job = client.query(
        query,
    )
    query_result = query_job.result()
    incident = []
    for row in query_result:
        temp_dict = {}
        for key, value in row.items():
            temp_dict[key.replace("_", " ")] = value
            if type(temp_dict[key.replace("_", " ")]) == datetime.datetime:
                temp_dict[key.replace("_", " ")] = temp_dict[
                    key.replace("_", " ")
                ].replace(tzinfo=None)
        incident.append(dict(temp_dict))

    # load incident data to sheet
    row_count = len(incident)
    df = pd.DataFrame(incident)
    df = df[
        [
            "Created",
            "Priority",
            "Vendor Case",
            "Incident Number",
            "Short Description",
            "Assignment Group",
            "Assignee",
            "State",
            "Tool",
            "Config Item",
            "Account",
            "Geography",
            "Country",
            "Category",
            "Sub Category",
            "Updated",
            "Updated By",
            "Updates Count",
            "Resolved",
            "Resolution Code",
            "Closed",
            "Time Worked",
            "Acknowledge Time",
            "Reassignment Count",
        ]
    ]
    ws = gsheet.worksheet("IncidentData")
    set_with_dataframe(ws, df)
    # print(row_count)
    sheetId = ws._properties["sheetId"]
    # print(sheetId)
    body = {
        "requests": [
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheetId,
                        "startRowIndex": 1,
                        "endRowIndex": 2,
                        "startColumnIndex": 24,
                        "endColumnIndex": 33,
                    },
                    "destination": {
                        "sheetId": sheetId,
                        "startRowIndex": 2,
                        "endRowIndex": row_count + 1,
                        "startColumnIndex": 24,
                        "endColumnIndex": 33,
                    },
                    "pasteType": "PASTE_FORMULA",
                }
            }
        ]
    }
    res = gsheet.batch_update(body)

    # clear incidentsladata worksheet
    ws = gsheet.worksheet("IncidentSLAData")
    del_rows = ws.row_count
    try:
        ws.delete_rows(3, del_rows + 1)
    except:
        pass
    res = gsheet.values_clear("'IncidentSLAData'!A2:G2")

    # fetch incident sla data
    query = query_01
    incident_number = []
    for row in incident:
        incident_number.append(row.get("Incident Number"))
    client = bigquery.Client(project=bq_job_project_id)
    query_job = client.query(
        query,
    )
    query_result = query_job.result()
    incident_sla = []
    for row in query_result:
        temp_dict = {}
        if row.get("Incident_Number") in incident_number:
            for key, value in row.items():
                temp_dict[key.replace("_", " ")] = value
                if type(temp_dict[key.replace("_", " ")]) == datetime.datetime:
                    temp_dict[key.replace("_", " ")] = temp_dict[
                        key.replace("_", " ")
                    ].replace(tzinfo=None)
            temp_dict["Consumed Time"] = (
                temp_dict["Consumed Time"] - datetime.datetime(1970, 1, 1)
            ).seconds
            incident_sla.append(dict(temp_dict))

    # load incident sla data to sheet
    df = pd.DataFrame(incident_sla)
    df = df[
        [
            "Incident Number",
            "SLA Target",
            "Stage",
            "Has Breached",
            "Start Time",
            "End Time",
            "Consumed Time",
        ]
    ]
    ws = gsheet.worksheet("IncidentSLAData")
    set_with_dataframe(ws, df)

    # clear incidentdata worksheet
    ws = gsheet.worksheet("IncidentSurveyData")
    del_rows = ws.row_count
    try:
        ws.delete_rows(3, del_rows + 1)
    except:
        pass
    res = gsheet.values_clear("'IncidentSurveyData'!A2:G2")

    # fetch incident survey data
    query = query_03
    incident_number = []
    for row in incident:
        incident_number.append(row.get("Incident Number"))
    client = bigquery.Client(project=bq_job_project_id)
    query_job = client.query(
        query,
    )
    query_result = query_job.result()
    survey = []
    for row in query_result:
        temp_dict = {}
        if row.get("Incident_Number") in incident_number:
            for key, value in row.items():
                temp_dict[key.replace("_", " ")] = value
                if type(temp_dict[key.replace("_", " ")]) == datetime.datetime:
                    temp_dict[key.replace("_", " ")] = temp_dict[
                        key.replace("_", " ")
                    ].replace(tzinfo=None)
            temp_dict["% Completed"] = temp_dict.pop("Completed")
            survey.append(dict(temp_dict))

    # load incident sla data to sheet
    if len(survey) > 0 :
        df = pd.DataFrame(survey)
        df = df[['Survey Instance', 'Incident Number', 'Triggered On', 'Taken On', 'Taken By', '% Completed', 'Overall Rating']]
        ws = gsheet.worksheet('IncidentSurveyData')
        set_with_dataframe(ws, df)

def MAIN(request):
    loadIncidentData()
    return "SUCCESS"
