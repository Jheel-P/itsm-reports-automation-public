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
SELECT b.task_display_value Case_Number, b.sla_target SLA_Target, b.stage_value Stage, b.has_breached_display_value Has_Breached, 
    b.start_time_value Start_Time, b.end_time_value End_Time, b.duration_value Consumed_Time
FROM 
    `searce-cms-production.servicenow.sn_customerservice_case` a,
    `searce-cms-production.servicenow.task_sla` b
WHERE
    a.number_display_value = b.task_display_value AND a.assignment_group_display_value != "Workspace Support" AND b.stage_value != "cancelled"
"""
query_02 = """
SELECT a.sys_created_on_value Created, a.priority_display_value Priority, a.u_vendor_ticket_value Vendor_Case, a.number_display_value Case_Number, 
    a.short_description_display_value Short_Description, a.assignment_group_display_value Assignment_Group, 
    a.assigned_to_display_value Assignee, a.state_display_value State, a.account_display_value Account, b.u_geography_display_value Geography,
    b.country_display_value Country, a.category_display_value Category, a.subcategory_display_value Sub_Category,
    a.sys_updated_on_value Updated, a.sys_updated_by_display_value Updated_By, a.sys_mod_count_value Updates_Count,
    a.resolved_at_value Resolved, a.resolution_code_display_value Resolution_Code, a.closed_at_value Closed, 0 Time_Worked, 
    a.u_acknowledged_time_value Acknowledge_Time, a.reassignment_count_value Reassignment_Count
FROM 
    `searce-cms-production.servicenow.sn_customerservice_case` a
LEFT JOIN 
    `searce-cms-production.servicenow.customer_account` b ON b.sys_id_value = a.account_value
WHERE
    a.assignment_group_display_value != "Workspace Support" AND a.parent_display_value IS NULL
    AND (a.sys_created_on_value >= TIMESTAMP_SUB(CURRENT_TIMESTAMP() , INTERVAL 90 DAY) 
    OR a.resolved_at_value >= TIMESTAMP_SUB(CURRENT_TIMESTAMP() , INTERVAL 90 DAY) OR a.resolved_at_value IS NULL)
"""
query_03 = """
SELECT b.number_value Survey_Instance, b.task_id_display_value Case_Number, b.sys_created_on_value Triggered_On,
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


def loadCaseData():
    # clear casedata worksheet
    ws = gsheet.worksheet("CaseData")
    del_rows = ws.row_count
    try:
        ws.delete_rows(3, del_rows + 1)
    except:
        pass
    res = gsheet.values_clear("'CaseData'!A2:V2")

    # fetch case data
    query = query_02
    client = bigquery.Client(project=bq_job_project_id)
    query_job = client.query(
        query,
    )
    query_result = query_job.result()
    case = []
    for row in query_result:
        temp_dict = {}
        for key, value in row.items():
            temp_dict[key.replace("_", " ")] = value
            if type(temp_dict[key.replace("_", " ")]) == datetime.datetime:
                temp_dict[key.replace("_", " ")] = temp_dict[
                    key.replace("_", " ")
                ].replace(tzinfo=None)
        case.append(dict(temp_dict))

    # load case data to sheet
    row_count = len(case)
    df = pd.DataFrame(case)
    df = df[
        [
            "Created",
            "Priority",
            "Vendor Case",
            "Case Number",
            "Short Description",
            "Assignment Group",
            "Assignee",
            "State",
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
    ws = gsheet.worksheet("CaseData")
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
                        "startColumnIndex": 22,
                        "endColumnIndex": 31,
                    },
                    "destination": {
                        "sheetId": sheetId,
                        "startRowIndex": 2,
                        "endRowIndex": row_count + 1,
                        "startColumnIndex": 22,
                        "endColumnIndex": 31,
                    },
                    "pasteType": "PASTE_FORMULA",
                }
            }
        ]
    }
    res = gsheet.batch_update(body)

    # clear casesladata worksheet
    ws = gsheet.worksheet("CaseSLAData")
    del_rows = ws.row_count
    try:
        ws.delete_rows(3, del_rows + 1)
    except:
        pass
    res = gsheet.values_clear("'CaseSLAData'!A2:G2")

    # fetch case sla data
    query = query_01
    case_number = []
    for row in case:
        case_number.append(row.get("Case Number"))
    client = bigquery.Client(project=bq_job_project_id)
    query_job = client.query(
        query,
    )
    query_result = query_job.result()
    case_sla = []
    for row in query_result:
        temp_dict = {}
        if row.get("Case_Number") in case_number:
            for key, value in row.items():
                temp_dict[key.replace("_", " ")] = value
                if type(temp_dict[key.replace("_", " ")]) == datetime.datetime:
                    temp_dict[key.replace("_", " ")] = temp_dict[
                        key.replace("_", " ")
                    ].replace(tzinfo=None)
            temp_dict["Consumed Time"] = (
                temp_dict["Consumed Time"] - datetime.datetime(1970, 1, 1)
            ).seconds
            case_sla.append(dict(temp_dict))

    # load case sla data to sheet
    df = pd.DataFrame(case_sla)
    df = df[
        [
            "Case Number",
            "SLA Target",
            "Stage",
            "Has Breached",
            "Start Time",
            "End Time",
            "Consumed Time",
        ]
    ]
    ws = gsheet.worksheet("CaseSLAData")
    set_with_dataframe(ws, df)

    # clear casedata worksheet
    ws = gsheet.worksheet("CaseSurveyData")
    del_rows = ws.row_count
    try:
        ws.delete_rows(3, del_rows + 1)
    except:
        pass
    res = gsheet.values_clear("'CaseSurveyData'!A2:G2")

    # fetch case survey data
    query = query_03
    case_number = []
    for row in case:
        case_number.append(row.get("Case Number"))
    client = bigquery.Client(project=bq_job_project_id)
    query_job = client.query(
        query,
    )
    query_result = query_job.result()
    survey = []
    for row in query_result:
        temp_dict = {}
        if row.get("Case_Number") in case_number:
            for key, value in row.items():
                temp_dict[key.replace("_", " ")] = value
                if type(temp_dict[key.replace("_", " ")]) == datetime.datetime:
                    temp_dict[key.replace("_", " ")] = temp_dict[
                        key.replace("_", " ")
                    ].replace(tzinfo=None)
            temp_dict["% Completed"] = temp_dict.pop("Completed")
            survey.append(dict(temp_dict))

    # load case sla data to sheet
    df = pd.DataFrame(survey)
    df = df[['Survey Instance', 'Case Number', 'Triggered On', 'Taken On', 'Taken By', '% Completed', 'Overall Rating']]
    ws = gsheet.worksheet('CaseSurveyData')
    set_with_dataframe(ws, df)

def MAIN(request):
    loadCaseData()
    return "SUCCESS"
