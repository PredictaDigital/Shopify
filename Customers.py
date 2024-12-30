import requests
import pyodbc
from datetime import datetime

# Shopify API credentials
SHOP_NAME = 'littlecheeks-com-au.myshopify.com'
API_VERSION = '2024-04'
ACCESS_TOKEN = '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'

# SQL Server database credentials
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
database_table = "dbo.Shopify_Littlecheeks_Discounts_KS"

# Establish connection to SQL Server database
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Truncate the table
cursor.execute(f'TRUNCATE TABLE {database_table}')
conn.commit()

# Initialize limit and since_id for pagination
limit = 250

# Determine the max updated_at present in the table
cursor.execute(f"SELECT ISNULL(MAX(updated_at), '2000-01-01') FROM {database_table}")
max_updated_at = cursor.fetchone()[0]
if isinstance(max_updated_at, datetime):
    max_updated_at = max_updated_at.strftime('%Y-%m-%dT%H:%M:%S%z')

def fetch_discounts():
    since_id = None  # Initialize cursor for pagination

    while True:
        price_rules_url = f'https://{SHOP_NAME}/admin/api/{API_VERSION}/price_rules.json'
        params = {'limit': limit}
        if since_id:
            params['since_id'] = since_id

        headers = {
            'X-Shopify-Access-Token': ACCESS_TOKEN,
            'Content-Type': 'application/json'
        }

        price_rules_response = requests.get(price_rules_url, params=params, headers=headers)
        if price_rules_response.status_code == 200:
            price_rules = price_rules_response.json().get('price_rules', [])
            if not price_rules:
                break  # No more price rules to process

            for price_rule in price_rules:
                process_price_rule(price_rule, headers)

            # Update since_id to the ID of the last price rule in the current batch
            since_id = price_rules[-1]['id']
        else:
            print(f"Failed to fetch price rules: {price_rules_response.status_code} - {price_rules_response.text}")
            break

def process_price_rule(price_rule, headers):
    price_rule_id = int(price_rule['id'])
    title = price_rule['title']
    target_type = price_rule['target_type']
    target_selection = price_rule['target_selection']
    allocation_method = price_rule['allocation_method']
    value_type = price_rule['value_type']
    value = float(price_rule['value'])
    once_per_customer = price_rule['once_per_customer']
    usage_limit = price_rule['usage_limit']
    starts_at = datetime.strptime(price_rule['starts_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
    ends_at = datetime.strptime(price_rule['ends_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S') if price_rule['ends_at'] else None
    created_at = datetime.strptime(price_rule['created_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
    updated_at = datetime.strptime(price_rule['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')

    rule_value_type = price_rule.get('value_type', None)
    rule_value = float(price_rule['value'])

    entitled_variant_ids = ','.join(map(str, price_rule.get('entitled_variant_ids', [])))
    entitled_product_ids = ','.join(map(str, price_rule.get('entitled_product_ids', [])))
    entitled_collection_ids = ','.join(map(str, price_rule.get('entitled_collection_ids', [])))

    discount_type = target_selection
    discount_method = value_type

    # Fetch discount codes for each price rule
    discount_codes_url = f'https://{SHOP_NAME}/admin/api/{API_VERSION}/price_rules/{price_rule_id}/discount_codes.json'
    discount_codes_response = requests.get(discount_codes_url, headers=headers)

    if discount_codes_response.status_code == 200:
        discount_codes = discount_codes_response.json().get('discount_codes', [])
        for discount_code in discount_codes:
            discount_code_id = int(discount_code['id'])
            code = discount_code['code']
            usage_count = int(discount_code['usage_count'])
            discount_id = int(discount_code['id'])

            # Check if record already exists
            if not record_exists(price_rule_id, discount_code_id):
                insert_discount(price_rule_id, title, target_type, target_selection, allocation_method, value_type, value,
                                once_per_customer, usage_limit, starts_at, ends_at, created_at, updated_at,
                                discount_code_id, code, usage_count, discount_type, discount_method,
                                rule_value_type, rule_value, entitled_variant_ids, entitled_product_ids, entitled_collection_ids, discount_id)
    else:
        print(f"Failed to fetch discount codes for price_rule_id {price_rule_id}. Error: {discount_codes_response.status_code} - {discount_codes_response.text}")

def record_exists(price_rule_id, discount_code_id):
    cursor.execute(f"SELECT COUNT(*) FROM {database_table} WHERE price_rule_id = ? AND discount_code_id = ?", (price_rule_id, discount_code_id))
    return cursor.fetchone()[0] > 0

def insert_discount(price_rule_id, title, target_type, target_selection, allocation_method, value_type, value,
                    once_per_customer, usage_limit, starts_at, ends_at, created_at, updated_at,
                    discount_code_id, code, usage_count, discount_type, discount_method,
                    rule_value_type, rule_value, entitled_variant_ids, entitled_product_ids, entitled_collection_ids, discount_id):
    try:
        insert_query = (
            f"INSERT INTO {database_table} (price_rule_id, title, target_type, target_selection, allocation_method, "
            f"value_type, value, once_per_customer, usage_limit, starts_at, ends_at, created_at, updated_at, "
            f"discount_code_id, code, usage_count, discount_type, discount_method, rule_value_type, rule_value, "
            f"entitled_variant_ids, entitled_product_ids, entitled_collection_ids, discount_id) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )

        cursor.execute(insert_query, (
            price_rule_id, title, target_type, target_selection, allocation_method, value_type, value,
            once_per_customer, usage_limit, starts_at, ends_at, created_at, updated_at,
            discount_code_id, code, usage_count, discount_type, discount_method,
            rule_value_type, rule_value, entitled_variant_ids, entitled_product_ids, entitled_collection_ids, discount_id
        ))
        conn.commit()
    except Exception as e:
        print(f"Failed to insert discount. Error: {str(e)}")

# Fetch and insert discounts
fetch_discounts()

# Close connections
cursor.close()
conn.close()
