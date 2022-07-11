import init
import sys
import json
from flask import Flask
from flask import request, Response
from modules import TableFormat, generateSysparmQuery, refreshOAuthToken, fetchFormatData, loadDataFromMemory, exclusion_fields_list, postProcess_v1, preFetchData, fetchDict, updateCheckpoint, uploadDumpToGCS

app = Flask('post method')

@app.route("/data_import", methods=["POST"])
def main () :
    json_body = request.json
    print(json_body)
    sys.stdout.flush()
    # json_body = {
    #     'bq_table': 'task_sla',
    #     'table': 'task_sla',
    #     'initial_import': False,
    #     'sysparm_limit': 1000,
    # }
    access_token = refreshOAuthToken()
    tableFormatter = TableFormat()

    input_dict = {
        'bq_table': json_body['bq_table'], # required
        'initial_import': fetchDict(json_body, 'initial_import', False),
    }
    sysparm_query, now = generateSysparmQuery(**input_dict)

    sys_parm = {
        'sysparm_query': sysparm_query,
        'sysparm_limit': fetchDict(json_body, 'sysparm_limit', 1000),
        'sysparm_display_value': 'all',
        # 'sysparm_fields': fields_list["incident_fields"]
    }
    if json_body['table'] == 'task_sla' :
        input_dict = {
            'table': json_body['table'],
            'access_token': access_token, # required
            'sys_parm': sys_parm, # required
            'table_formatter': tableFormatter.taskSLATable, # required
            'export_to_file': fetchDict(json_body, 'export_to_file', True), # optional
            'exclusion_list': fetchDict(exclusion_fields_list, json_body['table'], []),
            'sla_definations': preFetchData(access_token=access_token, table="contract_sla")
        }
    else :
        input_dict = {
            'table': json_body['table'],
            'access_token': access_token, # required
            'sys_parm': sys_parm, # required
            'table_formatter': tableFormatter.commonFormatter, # required
            'export_to_file': fetchDict(json_body, 'export_to_file', True), # optional
            'exclusion_list': ['comments','work_notes','u_commets1','u_text_comments','comments_and_work_notes','work_notes_list'],
        }
    data = fetchFormatData(**input_dict)

    load_to_gcs = fetchDict(json_body, 'export_to_gcs', True)
    if load_to_gcs :
        uploadDumpToGCS(now=now, dump_filename=f"{json_body['table']}_dump.jsonl", bq_table=json_body['bq_table'])

    input_dict = {
        'bq_table': json_body['bq_table'], # required
        'data': data, # required
    }
    loadDataFromMemory(**input_dict)

    postProcess_v1(bq_table=json_body['bq_table'])

    input_dict = {
        'bq_table': json_body['bq_table'], # required
        'table': json_body['table'], # required
        'last_updated': now
    }
    updateCheckpoint(**input_dict)

    return_res = {'message': 'success'}
    return Response(json.dumps(return_res), status=200, mimetype='application/json')

@app.route("/run_query", methods=["POST"])
def run_query () :
    json_body = request.json
    print(json_body)
    sys.stdout.flush()
    # json_body = {
    #     'source': 'gcs_blob',
    #     'gcs_bucket': 'BUCKET_NAME',
    #     'blob_name': 'BLOB_NAME',
    # }
    

if __name__ == '__main__':
    app.run(host = '0.0.0.0', port = 8080)

# main()

# input_dict = {
#     'dump_file': 'customer_account_dump.jsonl',
#     'schema_file': 'customer_account_schema.json'
# }
# generateSchema(**input_dict)
# runCommand(command=f"bq mk --table {init.bq_project}:{init.dataset_id}.customer_account customer_account_schema.json")

