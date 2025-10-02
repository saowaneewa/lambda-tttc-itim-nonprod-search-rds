'''
AWS Lambda function to search RDS MySQL database based on event_type and parameters.
---------------------------------------------------------
last updated:
[2024-08-29 16:00] TTS-T Saowanee) revise upd_timestamp query to exclude fg, mc, shipout
[2025-09-10 15:30] TTS-T Saowanee) apply boto3 logger to CloudWatch
[2025-09-17 14:00] TTS-T Saowanee) Revise timestamp & timestamp_fg & delete No.6 & 7
[2025-10-02 15:00] TTS-T Saowanee) Revise search_tag - search from new view TABLE_VIEW_TAGS #1
'''
import json
import pymysql
import os
import boto3
import sys
import logging
from datetime import datetime, timedelta
import traceback
import csv
from io import StringIO

# const
TABLE_KSSP = "KSSP_BAL_STOCK1"  # "KSSP_BAL_STOCK_testing"
TABLE_COMMON_SPEC = "COMMON_SPEC_CONFIG"
TABLE_DIRECT_SALE = "TTTC_DIRECT_SALE"
TABLE_IN_TRANSIT = "TTTC_IN_TRANSIT1"
TABLE_FORECAST = "CUSTOMER_FORECAST1"
TABLE_MONBAL_FG = "MONTHLY_MS2_FG"  # "CUTTINGCENTER_BAL_FG2"
TABLE_FG = "CUTTINGCENTER_BAL_FG"
TABLE_MC = "CUTTINGCENTER_BAL_MC"
UPD_TIMESTAMP = "UPD_TIMESTAMP"
TABLE_VIEW_TAGS = "STOCK_MILLSHEET_TAG"

# ===== boto3/botocore streaming logger (to CloudWatch via stdout) =====
# Control with env var BOTO3_LOG_LEVEL = DEBUG|INFO|WARNING|ERROR|CRITICAL (default: WARNING)
_BOTO3_LOG_LEVEL = os.getenv("BOTO3_LOG_LEVEL", "WARNING").upper()
_LOG_LEVEL = getattr(logging, _BOTO3_LOG_LEVEL, logging.WARNING)
boto3.set_stream_logger("boto3", level=_LOG_LEVEL)
boto3.set_stream_logger("botocore", level=_LOG_LEVEL)

# Get AWS account ID
account_name = ""
account_id = boto3.client('sts').get_caller_identity()['Account']
if account_id == "533267323749":
    account_name = account_id
elif account_id == "808329257923":
    account_name = "tttc-itim-nonprod"
elif account_id == "652058538377":
    account_name = "tttc-itim-prod"
logs_s3bucket = f"s3-{account_name}-processing-notifications"
errors_folder = "ERRORS/search-rds/"  #"TEST/ERRORS/search-rds/"

# Define a mapping of exceptions to HTTP status codes
exception_status_codes = {
    MemoryError: 413,  # Request Entity Too Large
    KeyError: 400,     # Bad Request
    ValueError: 422,   # Unprocessable Entity
    TypeError: 400,    # Bad Request
    TimeoutError: 504  # Gateway Timeout
    # Add more exceptions and corresponding status codes as needed
}

## =================================== get database secret ===================================
def get_secret():
    secret_name = os.getenv('DB_SECRET_NAME')
    region_name = "ap-southeast-7" ## Changed 8-Aug-25
    # Create a Secrets Manager client
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except client.exceptions.ResourceNotFoundException:
        print(f"The requested secret {secret_name} was not found")
        raise
    except client.exceptions.InvalidRequestException:
        print(f"The request was invalid due to: {secret_name}")
        raise
    except client.exceptions.InvalidParameterException:
        print(f"The request had invalid params: {secret_name}")
        raise
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    secret = get_secret_value_response['SecretString']
    return json.loads(secret)

secrets = get_secret()
db_host = secrets['host']
db_username = secrets['username']
db_password = secrets['password']
db_name = secrets['dbname']
## =================================== database secret end. ===================================

# Establish a connection to the MySQL database
try:
    conn = pymysql.connect(
        host=db_host,
        port=int(os.environ.get("DB_PORT",3306)), ## Add 6-Aug-25
        user=db_username,
        passwd=db_password,
        db=db_name,
        connect_timeout=10,
        ssl={"ca": "/opt/python/certs/rds-combined-ca-bundle.pem"}, ## Add 6-Aug-25
        autocommit=True,
        charset='utf8mb4' #,cursorclass=pymysql.cursors.DictCursor
    )
except pymysql.MySQLError as e:
    print("ERROR: Unexpected error: Could not connect to MySQL instance.")
    print(e)
    sys.exit()
print("SUCCESS: Connection to RDS MySQL instance succeeded")

## =================================== Common Functions. ===================================
def remove_quotes(text):
    return text.strip('"')

def check_missing_params(event, required_params):
    missing_params = [param for param in required_params if param not in event]
    if missing_params:
        print(f"Missing parameters: {', '.join(missing_params)}")
        return {
            'statusCode': 400,
            'errorMessage': json.dumps(f"Missing parameters: {', '.join(missing_params)}.")
        }
    return None

def format_customer_input(customer_input):
    customer_input = customer_input.strip()
    
    # Remove surrounding parentheses if present
    if customer_input.startswith('(') and customer_input.endswith(')'):
        customer_input = customer_input[1:-1]

    # Split by comma and clean each item
    customers_list = []
    reader = csv.reader(StringIO(customer_input), skipinitialspace=True)
    customers_list = next(reader)

    # Format output based on number of items 
    if len(customers_list) == 1:
        customers_tuple_str = f"('{customers_list[0].strip()}')"
    else:
        customers_tuple_str = str(tuple(customers_list))

    return customers_tuple_str

## =================================== AWS Lambda handler. ===================================
def lambda_handler(event, context):
    event_type = event['event_type']
    #print(f"event_type: {event_type}")

    # ---------- 1. search_tag ----------
    if event_type == "1":
        print("event_type: =======> 1. search_tag")
        required_params = ['CUSTOMER']
        error_response = check_missing_params(event, required_params)
        if error_response:
            return error_response
        PARM_CUST = event['CUSTOMER']
        print(f"CUSTOMER: {PARM_CUST}")

        formatted_cust = format_customer_input(PARM_CUST)

        EXECUTE_QUERY = f'''
	        SELECT * FROM {TABLE_VIEW_TAGS} '''
        WHERE_CONDITIONS = f'''
            WHERE customer IN {formatted_cust} '''

    # ---------- 2. search_directsale ----------
    elif event_type == "2":
        print("event_type: =======> 2. search_directsale")
        required_params = ['CUSTOMER']
        error_response = check_missing_params(event, required_params)
        if error_response:
            return error_response
        PARM_CUST = event['CUSTOMER']
        print(f"CUSTOMER: {PARM_CUST}")
        # Check if PARM_CUST is "AFT" and update it accordingly
        if PARM_CUST == "AFT":
            PARM_CUST = "AFT,AICHI FORGE"
        elif PARM_CUST == "COMMON SPEC":
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT customer FROM {TABLE_COMMON_SPEC} GROUP BY customer")
                customers = cursor.fetchall()
            PARM_CUST = ','.join([customer[0] for customer in customers])
            print(f"CUSTOMER: {PARM_CUST}")
        EXECUTE_QUERY = f'''
        SELECT ID, specification_, diameter, customer, `length`, `Customer Part no.`, Maker, WH, invoice_no, selling_price,
        inspection, package_no, content_no, nweight, Total, IFNULL(NULLIF(ETA, ""), "") AS ETA, LEFT(ETA, 6) AS ETA_Mon ,
        shipment, sc_no, intake_date, src_file_nm,
        DATE_FORMAT(intake_date,"%Y-%m-%d %H:%i:%S") as intake_date ,
                DATE_FORMAT(delivery_date,"%Y-%m-%d %H:%i:%S") as delivery_date ,
                DATE_FORMAT(actual_delivery,"%Y-%m-%d %H:%i:%S") as actual_delivery ,
                DATE_FORMAT(upd_timestamp,"%Y-%m-%d %H:%i:%S") as upd_timestamp
        FROM {TABLE_DIRECT_SALE}
        '''
        WHERE_CONDITIONS = f'''
        WHERE customer IN ({', '.join(f"'{cust.strip()}'" for cust in PARM_CUST.split(','))})
        '''

    # ---------- 3. search_intransit ----------
    elif event_type == "3":
        print("event_type: =======> 3. search_intransit")
        required_params = ['CUSTOMER_SHORTNAME']
        error_response = check_missing_params(event, required_params)
        if error_response:
            return error_response
        PARM_CUST = event['CUSTOMER_SHORTNAME']
        # Check if PARM_CUST is "AFT" and update it accordingly
        if PARM_CUST == "AFT":
            PARM_CUST = "AFT,AICHI FORGE"
        elif PARM_CUST == "COMMON SPEC":
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT customer FROM {TABLE_COMMON_SPEC} GROUP BY customer")
                customers = cursor.fetchall()
            PARM_CUST = ','.join([customer[0] for customer in customers])
            print(f"CUSTOMER: {PARM_CUST}")
        print(f"CUSTOMER_SHORTNAME: {PARM_CUST}")
        EXECUTE_QUERY = f'''
        SELECT intransit_month, customer, cust_shortname, invoice_no, SPEC, diameter, cust_part_no, maker, maker_list, `length`,
        inspection, package_no, content_no, nweight, nweight_round, Total, ETA,
        CONCAT(SPEC, "_", diameter, "_", IFNULL(cust_part_no, ""), IFNULL(maker, "")) as SPEC_KEYmaker,
        DATE_FORMAT(upd_timestamp,"%Y-%m-%d %H:%i:%S") as upd_timestamp, src_file_nm
        FROM (
            SELECT its.*, kssp.customer, kssp.customer as cust_shortname, maker_list
            FROM (
                SELECT LEFT(ETA, 6) as intransit_month, invoice_no, UPPER(specification_) as SPEC, inspection, package_no, content_no,
                IFNULL(maker, "") as maker, diameter, `length`,
                nweight, ROUND(nweight) as nweight_round, Total, IFNULL(`Customer Part no.`, "") as cust_part_no, ETA, upd_timestamp, src_file_nm
                FROM {TABLE_IN_TRANSIT}
            ) as its
            LEFT JOIN (
                SELECT customer, UPPER(specification_) as k_SPEC, diameter as k_DIA,
                IFNULL(`Customer Part no.`, "") as k_cust_part_no, GROUP_CONCAT(DISTINCT maker) as maker_list
                FROM {TABLE_KSSP}
                GROUP BY customer, k_SPEC, k_DIA, k_cust_part_no
            ) as kssp
            ON SPEC = k_SPEC AND diameter = k_DIA AND cust_part_no = k_cust_part_no
        ) as its_custom
        '''
        WHERE_CONDITIONS = f'WHERE cust_shortname IN ({", ".join(f"\'{cust.strip()}\'" for cust in PARM_CUST.split(","))}) '

    # ---------- 4. search_forecast ----------
    elif event_type == "4":
        print("event_type: =======> 4. search_forecast")
        required_params = ['CUSTOMER']
        error_response = check_missing_params(event, required_params)
        if error_response:
            return error_response
        PARM_CUST = event['CUSTOMER']
        # Check if PARM_CUST is "AFT" and update it accordingly
        if PARM_CUST == "AFT":
            PARM_CUST = "AFT,AICHI FORGE"
        elif PARM_CUST == "COMMON SPEC":
            with conn.cursor() as cursor:
                cursor.execute(f"SELECT customer FROM {TABLE_COMMON_SPEC} GROUP BY customer")
                customers = cursor.fetchall()
            PARM_CUST = ','.join([customer[0] for customer in customers])
            print(f"CUSTOMER: {PARM_CUST}")
        print(f"CUSTOMER_SHORTNAME: {PARM_CUST}")
        EXECUTE_QUERY = f'''
        SELECT * FROM (
            SELECT forecast.*, customer_name as cust_shortname, maker_list
            FROM (
                SELECT customer_name, as_of_month, specification, diameter, customer_part_no, maker, `length`,
                quantity_kg, forecast_kg, forecast_kg_N1, forecast_kg_N2, forecast_kg_N3, forecast_kg_N4, file_set_id, src_file_nm,
                DATE_FORMAT(upd_timestamp,"%Y-%m-%d %H:%i:%S") as upd_timestamp
                FROM {TABLE_FORECAST}
            ) as forecast
            LEFT JOIN (
                SELECT customer, UPPER(specification_) as SPEC, diameter, GROUP_CONCAT(DISTINCTROW maker) as maker_list
                FROM {TABLE_KSSP}
                GROUP BY customer, SPEC, diameter
            ) as kssp
            ON customer_name = customer AND forecast.specification = kssp.SPEC AND forecast.diameter = kssp.diameter
        ) t1
        '''
        WHERE_CONDITIONS = f'WHERE cust_shortname IN ({", ".join(f"\'{cust.strip()}\'" for cust in PARM_CUST.split(","))}) '

    # ---------- 5. get_upd_timestamp ----------
    elif event_type == "5":
        print("event_type: =======> 5. get_upd_timestamp")
        EXECUTE_QUERY = f"SELECT stamp_month, upd_timestamp, src_file_nm, src_table FROM {UPD_TIMESTAMP} "
        WHERE_CONDITIONS = "WHERE src_table NOT IN ('fg' , 'mc' , 'shipout') ORDER BY stamp_month DESC, upd_timestamp DESC LIMIT 1 ;"

    # ---------- 6. get_upd_timestamp_fg ----------
    elif event_type == "6":
        print("event_type: =======> 6. get_upd_timestamp_fg")
        EXECUTE_QUERY = f"SELECT stamp_month, upd_timestamp, src_file_nm, src_table FROM {UPD_TIMESTAMP} "
        WHERE_CONDITIONS = "WHERE src_table IN ('fg' , 'mc' , 'shipout') ORDER BY stamp_month DESC, upd_timestamp DESC LIMIT 1 ;"


    # ---------- 8. search_fg_mc_all_customers ----------
    elif event_type == "8":
        print(f"event_type: =======> {event_type}. search_fg_all_customers")
        EXECUTE_QUERY = f'''
WITH
    fg AS (
     SELECT center AS cutting_center, customer, 'fg' AS table_type
     FROM {TABLE_FG}
     GROUP BY cutting_center, customer
    ),
    mc AS (
     SELECT center AS cutting_center, customer, 'mc' AS table_type
     FROM {TABLE_MC}
     GROUP BY cutting_center, customer
    ),
    un AS (
        SELECT * FROM fg
        UNION ALL
        SELECT * FROM mc
    )
SELECT * FROM un
GROUP BY cutting_center, customer, table_type
        '''
        WHERE_CONDITIONS = " ORDER BY customer;"

    # --- execute SQL ---
    try:
        with conn.cursor() as cursor:
            cursor.execute(EXECUTE_QUERY + WHERE_CONDITIONS)
            cursor.connection.commit()
            # Extract row headers
            row_headers = [x[0] for x in cursor.description]
            # Fetch the results
            results = cursor.fetchall()
            # Format the results as a list of dictionaries
            json_data = [dict(zip(row_headers, row)) for row in results]
            # Get the number of affected rows
            affected_rows = cursor.rowcount
            if event_type == "5":
                print(json_data)
            return {
                'statusCode': 200,
                'count': affected_rows,
                'body': json_data
            }
    except Exception as e:
        # Prepare error log content
        errorMessage = f"""
'statusCode': 500,
'error': {str(e)},
'message': 'Query execution failed. Logged to S3.'
"""
        error_query = f'''
{errorMessage}
============
EXECUTE_QUERY:
{EXECUTE_QUERY} + {WHERE_CONDITIONS}
============
'''
        timestamp = (datetime.utcnow() + timedelta(hours=7)).strftime('%Y%m%d_%H%M%S')
        error_filename = f"{errors_folder}error_{timestamp}.txt"

        # Upload to S3
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=logs_s3bucket,
            Key=error_filename,
            Body=error_query.encode('utf-8')
        )
        print(f"Query execution failed. Logged to S3 as {error_filename}")
        return {
            'statusCode': 500,
            'error': str(e),
            'message': 'Query execution failed. Logged to S3.'
        }
