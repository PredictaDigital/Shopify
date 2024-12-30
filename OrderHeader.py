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
database_table = "dbo.Shopify_Littlecheeks_Abandoned_Checkouts_KS"

# Establish connection to SQL Server database
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Check for the maximum updated_at date
cursor.execute(f"SELECT MAX(updated_at) FROM {database_table}")
max_updated_at = cursor.fetchone()[0]

# If max_updated_at is None, this is the initial full load
if max_updated_at:
    # Convert max_updated_at to the format required by the API
    max_updated_at_str = max_updated_at.strftime('%Y-%m-%dT%H:%M:%S%z')
else:
    max_updated_at_str = None

# Initialize variables for pagination
checkouts_data = []
since_id = None
page = 1

while True:
    # Construct the API endpoint URL
    endpoint = f'https://{SHOP_NAME}/admin/api/{API_VERSION}/checkouts.json'
    params = {'limit': 250}
    if max_updated_at_str:
        params['updated_at_min'] = max_updated_at_str  # Incremental load
    if since_id:
        params['since_id'] = since_id
    headers = {
        'X-Shopify-Access-Token': ACCESS_TOKEN,
        'Content-Type': 'application/json'
    }

    response = requests.get(endpoint, params=params, headers=headers)

    if response.status_code == 200:
        # Extract checkout data from the current page
        page_checkouts_data = response.json().get('checkouts', [])
        if not page_checkouts_data:
            break  # Exit if no checkouts are returned

        checkouts_data.extend(page_checkouts_data)

        # Update since_id to the ID of the last checkout in the current page
        since_id = page_checkouts_data[-1]['id']
        page += 1
    else:
        print(f"Failed to fetch checkouts: {response.status_code} - {response.text}")
        break

# SQL query to insert data into abandoned_checkouts table
insert_query = f"""
    INSERT INTO {database_table} (checkout_id, cart_token, email, created_at, updated_at, completed_at, 
                                  abandoned_checkout_url, currency, customer_id, customer_first_name, 
                                  customer_last_name, total_price, subtotal_price, total_discounts, 
                                  total_tax, total_shipping, shipping_address, shipping_city, 
                                  shipping_province, shipping_zip, shipping_country, billing_address, 
                                  billing_city, billing_province, billing_zip, billing_country, 
                                  product_name, product_price, product_quantity, product_sku)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Iterate through each checkout and insert into the database
for checkout in checkouts_data:
    checkout_id = checkout['id']
    cart_token = checkout['cart_token']
    email = checkout['email']
    created_at = datetime.strptime(checkout['created_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
    updated_at = datetime.strptime(checkout['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')

    completed_at = checkout['completed_at']
    if completed_at and completed_at.lower() != 'null':
        completed_at = datetime.strptime(completed_at, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
    else:
        completed_at = None

    abandoned_checkout_url = checkout['abandoned_checkout_url']
    currency = checkout['currency']

    customer_id = checkout['customer']['id'] if checkout['customer'] else None
    customer_first_name = checkout['customer']['first_name'] if checkout['customer'] else None
    customer_last_name = checkout['customer']['last_name'] if checkout['customer'] else None

    total_price = checkout['total_price']
    subtotal_price = checkout['subtotal_price']
    total_discounts = checkout['total_discounts']
    total_tax = checkout['total_tax']
    total_shipping = checkout.get('total_shipping_price_set', {}).get('shop_money', {}).get('amount', '0')

    # Shipping address details
    shipping_address = f"{checkout['shipping_address'].get('address1', '')} {checkout['shipping_address'].get('address2', '')}" if checkout['shipping_address'] else None
    shipping_city = checkout['shipping_address'].get('city', '') if checkout['shipping_address'] else None
    shipping_province = checkout['shipping_address'].get('province', '') if checkout['shipping_address'] else None
    shipping_zip = checkout['shipping_address'].get('zip', '') if checkout['shipping_address'] else None
    shipping_country = checkout['shipping_address'].get('country', '') if checkout['shipping_address'] else None

    # Billing address details
    billing_address = f"{checkout['billing_address'].get('address1', '')} {checkout['billing_address'].get('address2', '')}" if checkout['billing_address'] else None
    billing_city = checkout['billing_address'].get('city', '') if checkout['billing_address'] else None
    billing_province = checkout['billing_address'].get('province', '') if checkout['billing_address'] else None
    billing_zip = checkout['billing_address'].get('zip', '') if checkout['billing_address'] else None
    billing_country = checkout['billing_address'].get('country', '') if checkout['billing_address'] else None

    # Extract line items (products) and insert a row for each product
    for item in checkout.get('line_items', []):
        product_name = item['title']
        product_price = item['price']
        product_quantity = item['quantity']
        product_sku = item.get('sku', '')  # Extract SKU or use an empty string if not present

        # Execute the insert query for each line item
        values = (checkout_id, cart_token, email, created_at, updated_at, completed_at,
                  abandoned_checkout_url, currency, customer_id, customer_first_name,
                  customer_last_name, total_price, subtotal_price, total_discounts,
                  total_tax, total_shipping, shipping_address, shipping_city,
                  shipping_province, shipping_zip, shipping_country, billing_address,
                  billing_city, billing_province, billing_zip, billing_country,
                  product_name, product_price, product_quantity, product_sku)

        cursor.execute(insert_query, values)
        conn.commit()

# Close database connection
cursor.close()
conn.close()
