import requests
import pyodbc
from datetime import datetime
import re
import logging
import time

# Setup logging
logging.basicConfig(level=logging.INFO, filename='shopify_customers_import.log',
                    filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')

# Shopify API credentials
shop_name = 'littlecheeks-com-au.myshopify.com'
api_version = '2024-04'
access_token = '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'

# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
customers_table = "dbo.Shopify_Littlecheeks_Customers_KS"

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()
# Truncate the table
cursor.execute(f'TRUNCATE TABLE {customers_table}')
conn.commit()
# Fetch existing customer IDs to avoid duplicates
cursor.execute(f"SELECT customer_id FROM {customers_table}")
existing_customer_ids = {row[0] for row in cursor.fetchall()}

# Initialize variables for pagination and retries
next_page_cursor = None
retry_count = 0
max_retries = 5
batch_data = []

# Define batch size
BATCH_SIZE = 100

while True:
    try:
        # Construct the API endpoint URL
        endpoint = f'https://{shop_name}/admin/api/{api_version}/customers.json'
        params = {'limit': 250}
        headers = {
            'X-Shopify-Access-Token': access_token,
            'Content-Type': 'application/json'
        }

        # Include cursor parameter if available
        if next_page_cursor:
            params['page_info'] = next_page_cursor

        response = requests.get(endpoint, params=params, headers=headers)

        # Check if the request was successful
        if response.status_code == 200:
            # Reset retry count on success
            retry_count = 0

            # Extract customer data from the current page
            customers_data = response.json().get('customers', [])
            if not customers_data:
                break  # Exit loop if no more data

            # Process each customer
            for customer in customers_data:
                customer_id = customer['id']
                if customer_id in existing_customer_ids:
                    continue  # Skip already existing customers
                existing_customer_ids.add(customer_id)  # Add to set to prevent future duplicates

                email = customer.get('email')
                created_at = datetime.strptime(customer['created_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
                updated_at = datetime.strptime(customer['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
                first_name = customer.get('first_name')
                last_name = customer.get('last_name')
                orders_count = customer['orders_count']
                state = customer['state']
                total_spent = float(customer['total_spent']) if customer['total_spent'] else 0.0
                last_order_id = customer.get('last_order_id')
                note = customer.get('note')
                verified_email = int(customer['verified_email'])
                multipass_identifier = customer.get('multipass_identifier')
                tax_exempt = int(customer['tax_exempt'])
                tags = customer.get('tags')
                last_order_name = customer.get('last_order_name')
                currency = customer.get('currency')
                phone = customer.get('phone')

                # Handling nested email_marketing_consent
                email_marketing_consent = customer.get('email_marketing_consent') or {}
                email_marketing_state = email_marketing_consent.get('state')
                email_marketing_opt_in_level = email_marketing_consent.get('opt_in_level')
                email_marketing_consent_updated_at = email_marketing_consent.get('consent_updated_at')
                if email_marketing_consent_updated_at:
                    email_marketing_consent_updated_at = datetime.strptime(
                        email_marketing_consent_updated_at, '%Y-%m-%dT%H:%M:%S%z'
                    ).strftime('%Y-%m-%d %H:%M:%S')

                # Handling nested sms_marketing_consent
                sms_marketing_consent = customer.get('sms_marketing_consent') or {}
                sms_consent_state = sms_marketing_consent.get('state')
                sms_consent_opt_in_level = sms_marketing_consent.get('opt_in_level')
                sms_consent_updated_at = sms_marketing_consent.get('consent_updated_at')
                if sms_consent_updated_at:
                    sms_consent_updated_at = datetime.strptime(
                        sms_consent_updated_at, '%Y-%m-%dT%H:%M:%S%z'
                    ).strftime('%Y-%m-%d %H:%M:%S')
                sms_consent_collected_from = sms_marketing_consent.get('consent_collected_from')

                # Handling nested addresses (assuming the first one for simplicity)
                addresses = customer.get('addresses', [])
                if addresses:
                    address = addresses[0]
                    address_id = address.get('id')
                    address_customer_id = address.get('customer_id')
                    address_first_name = address.get('first_name')
                    address_last_name = address.get('last_name')
                    address_company = address.get('company')
                    address_address1 = address.get('address1')
                    address_address2 = address.get('address2')
                    address_city = address.get('city')
                    address_province = address.get('province')
                    address_country = address.get('country')
                    address_zip = address.get('zip')
                    address_phone = address.get('phone')
                    address_name = address.get('name')
                    address_province_code = address.get('province_code')
                    address_country_code = address.get('country_code')
                    address_default = int(address.get('default', False))
                else:
                    # Set default values if addresses list is empty
                    address_id = None
                    address_customer_id = None
                    address_first_name = None
                    address_last_name = None
                    address_company = None
                    address_address1 = None
                    address_address2 = None
                    address_city = None
                    address_province = None
                    address_country = None
                    address_zip = None
                    address_phone = None
                    address_name = None
                    address_province_code = None
                    address_country_code = None
                    address_default = None

                admin_graphql_api_id = customer.get('admin_graphql_api_id')

                # Append to batch data
                batch_data.append((
                    customer_id, email, created_at, updated_at, first_name, last_name, orders_count, state, total_spent,
                    last_order_id, note, verified_email, multipass_identifier, tax_exempt, tags, last_order_name, currency,
                    phone, email_marketing_state, email_marketing_opt_in_level, email_marketing_consent_updated_at,
                    admin_graphql_api_id, address_id, address_customer_id, address_first_name, address_last_name,
                    address_company, address_address1, address_address2, address_city, address_province, address_country,
                    address_zip, address_phone, address_name, address_province_code, address_country_code, address_default,
                    sms_consent_state, sms_consent_opt_in_level, sms_consent_updated_at, sms_consent_collected_from
                ))

            # Insert batch data if batch size is reached
            if len(batch_data) >= 0:
                insert_customer_query = (
                    f"INSERT INTO {customers_table} (customer_id, email, created_at, updated_at, first_name, last_name, "
                    f"orders_count, state, total_spent, last_order_id, note, verified_email, multipass_identifier, "
                    f"tax_exempt, tags, last_order_name, currency, phone, email_marketing_state, email_marketing_opt_in_level, "
                    f"email_marketing_consent_updated_at, admin_graphql_api_id, address_id, address_customer_id, address_first_name, "
                    f"address_last_name, address_company, address_address1, address_address2, address_city, address_province, "
                    f"address_country, address_zip, address_phone, address_name, address_province_code, address_country_code, "
                    f"address_default, sms_consent_state, sms_consent_opt_in_level, sms_consent_updated_at, sms_consent_collected_from) "
                    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                )
                try:
                    cursor.executemany(insert_customer_query, batch_data)
                    conn.commit()
                    logging.info(f"Inserted {len(batch_data)} customers.")
                    batch_data.clear()  # Clear the batch data after insertion
                except pyodbc.IntegrityError as e:
                    logging.error(f"Duplicate entry encountered: {e}")

            # Extract the `link` header for pagination info
            link_header = response.headers.get('Link')
            if link_header:
                match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
                if match:
                    next_page_url = match.group(1)
                    next_page_cursor = re.search(r'page_info=([^&]+)', next_page_url).group(1)
                else:
                    break  # Exit loop if no pagination info is found
            else:
                break  # Exit loop if no more pages

        else:
            logging.error(f"Error fetching data: {response.status_code} {response.text}")
            retry_count += 1
            if retry_count >= max_retries:
                break
            time.sleep(5 * retry_count)  # Exponential backoff
            continue

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        retry_count += 1
        if retry_count >= max_retries:
            break
        time.sleep(5 * retry_count)  # Exponential backoff
        continue

# Insert any remaining data in the batch
if batch_data:
    insert_customer_query = (
        f"INSERT INTO {customers_table} (customer_id, email, created_at, updated_at, first_name, last_name, "
        f"orders_count, state, total_spent, last_order_id, note, verified_email, multipass_identifier, "
        f"tax_exempt, tags, last_order_name, currency, phone, email_marketing_state, email_marketing_opt_in_level, "
        f"email_marketing_consent_updated_at, admin_graphql_api_id, address_id, address_customer_id, address_first_name, "
        f"address_last_name, address_company, address_address1, address_address2, address_city, address_province, "
        f"address_country, address_zip, address_phone, address_name, address_province_code, address_country_code, "
        f"address_default, sms_consent_state, sms_consent_opt_in_level, sms_consent_updated_at, sms_consent_collected_from) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    try:
        cursor.executemany(insert_customer_query, batch_data)
        conn.commit()
        logging.info(f"Inserted remaining {len(batch_data)} customers.")
    except pyodbc.IntegrityError as e:
        logging.error(f"Duplicate entry encountered: {e}")

# Close the connection
cursor.close()
conn.close()
