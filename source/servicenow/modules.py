import json
import init
from subprocess import PIPE, Popen
import requests
import io
import traceback
import sys
from pprint import pprint
import datetime
import pytz
from google.cloud import bigquery, datastore, storage
from dotenv import load_dotenv
import collections
from bigquery_schema_generator.generate_schema import SchemaGenerator
from google.auth import default
from google.auth.transport.requests import Request
credentials, project = default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
credentials.refresh(Request())
oauth_token = credentials.token

def flatten(d, parent_key='', sep='_'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def fetchDict (dict, key, alt) :
    if key in dict :
        return dict[key]
    return alt

def runQuery (query) :
    from google.cloud import bigquery
    client = bigquery.Client(project=init.bq_project)
    query_job = client.query(query,)
    for row in query_job.result() :
        print(row)

def refreshOAuthToken () :
    url = init.instance_uri + '/oauth_token.do'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    request_body = {
        'grant_type': 'refresh_token',
        'redirect_uri': init.instance_uri + '/login.do',
        'client_id': init.client_id,
        'client_secret': init.client_secret,
        'refresh_token': init.refresh_token
    }
    response = requests.post(url, headers=headers, data=request_body)
    if "access_token" in response.json() :
        access_token = response.json()["access_token"]
    return access_token

def generateSysparmQuery (**kwargs) :
    # input_dict = {
    #     'bq_table': '(str) BQ_TABLE_WHERE_DATA_WILL_BE_LOADED_TO', # required
    #     'initial_import': '(bool) TRUE_TO_FETCH_DATA_FROM_START', # optional
    #     'now': '(datetime) LAST_UPDATE_DATETIME_TO_CONSIDER_FOR_LOADING_DATA', # optional
    #     'pre': '(datetime) UPDATE_MINIMUM_DATETIME_TO_CONSIDER_FOR_LOADING_DATA', # optional
    # }
    datastore_client = datastore.Client(project=init.bq_project)
    kind = "servicenow"
    name = f"{kwargs.get('bq_table')}"
    task_key = datastore_client.key(kind, name)
    entity = datastore_client.get(key=task_key)
    pre = kwargs.get('pre', entity.get("last_updated").astimezone(pytz.utc))
    now = kwargs.get('now', datetime.datetime.utcnow())
    now_date = str(now.date())
    now_time = str(now.time()).split(".")[0]
    pre_date = str(pre.date())
    pre_time = str(pre.time()).split(".")[0]
    # print(now_date, now_time, pre_date, pre_time)
    # sys_updated_on>=javascript:gs.dateGenerate('2021-12-08','00:00:00')^sys_updated_on<=javascript:gs.dateGenerate('2021-12-11','23:59:59')
    if kwargs.get("initial_import") :
        # self.sysparm_query = f"ORDERBYDESCsys_updated_on^sys_updated_on>=javascript:gs.dateGenerate('2021-12-14','00:00:00')^sys_updated_on<=javascript:gs.dateGenerate('{now_date}','{now_time}')"
        sysparm_query = f"ORDERBYDESCsys_updated_on^sys_updated_on<=javascript:gs.dateGenerate('{now_date}','{now_time}')"
    else :
        sysparm_query = f"ORDERBYsys_updated_on^sys_updated_on>=javascript:gs.dateGenerate('{pre_date}','{pre_time}')^sys_updated_on<=javascript:gs.dateGenerate('{now_date}','{now_time}')"
    # print(sysparm_query)
    now = now
    return sysparm_query, now

def preFetchData (**kwargs) :
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Bearer ' + kwargs.get('access_token')
    }
    url = init.instance_uri + f"/api/now/table/{kwargs.get('table')}"
    response = requests.get(url, headers=headers)
    response_json = response.json()
    contract_sla = []
    for result in response_json["result"] :
        temp_dict = {
            "contract_sla_sys_id": result["sys_id"],
            "contract_sla_name": result["name"],
            "sla_target": result["target"]
        }
        contract_sla.append(temp_dict)
    return contract_sla

def fetchFormatData (**kwargs) :
    # input_dict = {
    #     'table': '(str) SNOW_TABLE', # required
    #     'access_token': '(str) SNOW_API_ACCESS_TOKEN', # required
    #     'sys_parm': '(dict) REQUEST_QUERY_PARAMETERS', # required
    #     'table_formatter': '(func) DATA_FORMATTING_FUNCTION', # required
    #     'export_to_file': '(bool) EXPORT_DATA_TO_FILE_ALSO', # optional
    #     'export_file': '(str) FILENAME_TO_EXPORT_DATA_TO_IF_EXPORT_TO_FILE_IS_ENABLED', # optional
    # }
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': 'Bearer ' + kwargs.get('access_token')
    }
    page = 0
    total_records = 0
    offset = 0
    data = ""
    sys_parm = kwargs.get('sys_parm', {})
    while True :
        sys_parm["sysparm_offset"] = str(offset)
        # pprint(sys_parm)
        table = kwargs.get('table')
        url = init.instance_uri + f"/api/now/table/{table}"
        response = requests.get(url, headers=headers, params=sys_parm)
        response_json = response.json()
        if not ('result' in response_json) :
            print(response_json)
            sys.stdout.flush()
            return 1
        if len(response_json["result"]) == 0 :
            break
        page += 1
        offset += len(response_json["result"])
        for result in response_json["result"] :
            temp_map = kwargs.get('table_formatter')(json_data=result, **kwargs)
            data += json.dumps(temp_map) + '\n'
            total_records += 1
    if kwargs.get('export_to_file', True) :
        with open(f"{kwargs.get('export_file', f'{table}_dump.jsonl')}", "w+") as file :
            file.write(data)
    print("Total {} rows fetched.".format(total_records))
    sys.stdout.flush()
    return data

def loadDataFromMemory (**kwargs) :
    # input_dict = {
    #     'bq_table': '(str) BQ_TABLE_WHERE_DATA_WILL_BE_LOADED_TO', # required
    #     'data': '(str) NDJSON_DATA_TO_LOAD_INTO_BQ_TABLE', # required
    # }
    load_dotenv()
    client = bigquery.Client(project=init.bq_project)
    dataset_id = init.dataset_id
    table_id = f"{kwargs.get('bq_table')}"
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=False
    )
    job = client.load_table_from_file(
        # open(kwargs.get('import_file'), 'rb'),
        io.StringIO(kwargs.get('data')),
        table_ref,
        location=init.dataset_location,
        job_config=job_config,
    )
    job.result()
    print("Loaded {} rows into {}:{}.".format(job.output_rows, dataset_id, table_id))
    sys.stdout.flush()
    return 0

def updateCheckpoint (**kwargs) :
    # input_dict = {
    #     'bq_table': '(str) BQ_TABLE_WHERE_DATA_WILL_BE_LOADED_TO', # required
    #     'table': '(str) SNOW_TABLE', # required
    #     'last_updated': '(datetime) LAST_UPDATE_DATETIME', # required
    # }
    from google.cloud import datastore
    datastore_client = datastore.Client(project=init.bq_project)
    kind = "servicenow"
    name = kwargs.get('bq_table')
    task_key = datastore_client.key(kind, name)
    entity = datastore.Entity(key=task_key)
    entity.update({
        "last_updated": kwargs.get('last_updated'),
        "bq_table": name,
        "snow_table": kwargs.get('table')
    })
    datastore_client.put(entity)

class TableFormat :
    def noFormat (self, **kwargs) :
        return kwargs.get('json_data', None)
    def commonFormatter (self, **kwargs) :
        result = kwargs.get('json_data', None)
        exclusion_keys = kwargs.get('exclusion_list', [])
        for key in exclusion_keys :
            if key in result :
                del result[key]
        temp_dict = flatten(result)
        ret_dict = temp_dict.copy()
        for key, value in temp_dict.items() :
            if temp_dict[key] == "" :
                del ret_dict[key]
        return ret_dict
    def taskSLATable (self, **kwargs) :
        result = kwargs.get('json_data', None)
        exclusion_keys = kwargs.get('exclusion_list', [])
        for key in exclusion_keys :
            if key in result :
                del result[key]
        temp_dict = flatten(result)
        ret_dict = temp_dict.copy()
        for key, value in temp_dict.items() :
            if temp_dict[key] == "" :
                del ret_dict[key]
                continue
            if key == "sla_link" :
                for contract in kwargs.get("sla_definations", []) :
                    if value.split('/')[-1] == contract["contract_sla_sys_id"] :
                        ret_dict["sla_target"] = contract["sla_target"]
        return ret_dict

def generateSchema (**kwargs) :
    input_dict = {
        'dump_file': '(str) FILENAME_CONTAINING_THE_NDJSON_DATA_DUMP', # required
        'schema_file': '(str) OUTPUT_FILENAME_TO_EXPORT_SCHEMA', # required
    }
    generator = SchemaGenerator(input_format="json", keep_nulls=True)
    generator.run(input_file=open(kwargs.get('dump_file')), output_file=open(kwargs.get('schema_file'), "w+"))

def runCommand (**kwargs) :
    command = kwargs.get('command')
    p = Popen(command, stdout=PIPE, stderr=PIPE, shell=True)
    stdout, stderr = p.communicate()
    utf_error = stderr.decode("utf-8")
    # print(utf_error)
    utf_output = stdout.decode("utf-8")
    # print(utf_output)
    temp_dict = {
        "message": "COMMAND EXECUTION OUTPUT",
        "command": command,
        "error": utf_error,
        "output": utf_output
    }
    pprint(temp_dict)

def getBQTableSchema (**kwargs) :
    url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{kwargs.get['project']}/datasets/{kwargs.get['dataset']}/tables/{kwargs.get['table']}"
    headers = {
        "Authorization" : f"Bearer {oauth_token}"
    }
    response = requests.get(url, headers=headers)
    return response.json()['schema']['fields']

def postProcess_v1 (**kwargs) :
    table = f"{init.bq_project}.{init.dataset_id}.{kwargs.get('bq_table', None)}"
    print(table)
    query = f"""DELETE FROM `{table}`
    WHERE STRUCT(sys_id_value, sys_updated_on_value) NOT IN (
        SELECT AS STRUCT sys_id_value, MAX(sys_updated_on_value) sys_updated_on_value
        FROM `{table}`
        GROUP BY sys_id_value)"""
    runQuery(query=query)
    query = f"""CREATE OR REPLACE TABLE `{table}`
    AS
    SELECT DISTINCT * FROM `{table}`"""
    runQuery(query=query) 

def uploadDumpToGCS (**kwargs) :
    # input_dict = {
    #     "now": "(datetime)",
    #     "dump_filename": "(str) FILENAME_OF_THE_DUMP",
    #     "bq_table": "(str) BQ table name",
    # }
    storage_client = storage.Client()
    bucket = storage_client.bucket(init.dump_gcs_bucket)
    now = kwargs.get("now")
    key = f"{init.dump_gcs_path}{kwargs.get('bq_table')}/{now.year}/{now.strftime('%m')}/{now.strftime('%d')}/{now.strftime('%Y-%m-%dT%H:%M:%S')}.jsonl"
    blob = bucket.blob(key)
    blob.upload_from_filename(kwargs.get("dump_filename"))
    print(
        "File {} uploaded to {}.".format(
            kwargs.get("dump_filename"), init.dump_gcs_bucket + "/" + key
        )
    )
    sys.stdout.flush()

fields_list = {
    'incident_fields': 'parent,u_work_hours,u_disable_notifications,caused_by,watch_list,upon_reject,sys_updated_on,approval_history,skills,number,u_first_level_of_resolution,state,sys_created_by,knowledge,order,cmdb_ci,delivery_plan,impact,contract,active,priority,sys_domain_path,u_next_follow_up_date,business_duration,group_list,approval_set,universal_request,short_description,correlation_display,work_start,delivery_task,u_acknowledgement_time,additional_assignee_list,u_searce1,u_searce2,u_searce3,u_searce_field,notify,sys_class_name,service_offering,closed_by,follow_up,parent_incident,reopened_by,u_target_date,reassignment_count,assigned_to,sla_due,escalation,upon_approval,correlation_id,made_sla,child_incidents,hold_reason,task_effective_number,u_outage,resolved_by,sys_updated_by,u_first_response,opened_by,user_input,u_searce1left,u_monitoring_tool,sys_created_on,sys_domain,route_reason,calendar_stc,closed_at,business_service,rfc,time_worked,expected_start,opened_at,work_end,reopened_time,resolved_at,caller_id,u_working_hours,subcategory,close_code,assignment_group,business_stc,u_vendor_ticket,description,u_account,calendar_duration,close_notes,sys_id,contact_type,u_customer_response,incident_state,urgency,problem_id,u_channel,company,activity_due,severity,u_account_name,approval,due_date,sys_mod_count,reopen_count,sys_tags,u_customer_account,u_knowledge_article,location,category',
}

exclusion_fields_list = {
    'incident': ['comments','work_notes','u_commets1','u_text_comments','comments_and_work_notes','work_notes_list'],
    'sn_customerservice_case': ['work_notes_list','comments_and_work_notes','work_notes','description','comments','u_commets1','u_text_comments']
}