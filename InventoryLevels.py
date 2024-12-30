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
database_table = "dbo.Shopify_Littlecheeks_OrderLines_KS"

# Establish connection to SQL Server database
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Get the maximum updated_at date from the database
cursor.execute(f"SELECT MAX(updated_at) FROM {database_table}")
max_updated_at = cursor.fetchone()[0]

# If max_updated_at is None, this is the initial full load
if max_updated_at:
    max_updated_at_str = max_updated_at.strftime('%Y-%m-%dT%H:%M:%S%z')
else:
    max_updated_at_str = None

# Initialize variables for pagination
orders_data = []
since_id = None
page = 1

while True:
    # Construct the API endpoint URL
    endpoint = f'https://{SHOP_NAME}/admin/api/{API_VERSION}/orders.json?status=any'
    params = {
        "limit": 250,  # max limit per request
        "status": "any",  # to include all order statuses
        "order": "id asc"
    }
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
        # Extract order data from the current page
        page_orders_data = response.json().get('orders', [])
        if not page_orders_data:
            break  # Exit if no orders are returned

        orders_data.extend(page_orders_data)
        since_id = page_orders_data[-1]['id']
        page += 1
    else:
        print(f"Failed to fetch orders: {response.status_code} - {response.text}")
        break

# Iterate through orders_data and insert each order's line items into SQL
for order in orders_data:
    try:
        order_id = order['id']
        updated_at = datetime.strptime(order['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
        financial_status = order.get('financial_status', '')

        # Iterate through line items
        for item in order['line_items']:
            line_item_id = item.get('id', None)
            name = item.get('name', '')
            product_id = item.get('product_id', None)
            variant_id = item.get('variant_id', None)
            quantity = item.get('quantity', 0)
            price = item.get('price', '0')
            discount_allocations = item.get('discount_allocations', [])
            if discount_allocations:
                total_discount = sum(float(d['amount']) for d in discount_allocations)  # Sum of all discounts
            else:
                total_discount = 0  # Default to 0 if no discounts
            requires_shipping = item.get('requires_shipping', False)
            fulfillment_service = item.get('fulfillment_service', '')
            sku = item.get('sku', '')
            vendor = item.get('vendor', '')
            title = item.get('title', '')
            variant_title = item.get('variant_title', '')
            grams = item.get('grams', 0)

            current_quantity = item.get('current_quantity', 0)  # Extract current_quantity
            fulfillable_quantity = item.get('fulfillable_quantity', 0)  # Extract fulfillable_quantity

            taxable = item.get('taxable', False)
            gift_card = item.get('gift_card', False)
            product_exists = item.get('product_exists', True)
            admin_graphql_api_id = item.get('admin_graphql_api_id', '')
            variant_inventory_management = item.get('variant_inventory_management', '')

            # Define the INSERT query for line items
            insert_query = (
                f"INSERT INTO {database_table} ([order_id], [id], [name], [product_id], [variant_id], [quantity], [price], "
                f"[total_discount], [financial_status], [requires_shipping], [fulfillment_service], [sku], [vendor], [title], "
                f"[variant_title], [grams], [current_quantity], [fulfillable_quantity], [taxable], [gift_card], "
                f"[product_exists], [admin_graphql_api_id], [variant_inventory_management], [updated_at]) "
                f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )

            # Execute the query for each line item
            cursor.execute(insert_query, (order_id, line_item_id, name, product_id, variant_id, quantity, price,
                                          total_discount, financial_status, requires_shipping, fulfillment_service, sku,
                                          vendor, title, variant_title, grams, current_quantity, fulfillable_quantity,
                                          taxable, gift_card, product_exists, admin_graphql_api_id, variant_inventory_management,
                                          updated_at))
        conn.commit()

    except Exception as e:
        print(f"Failed to insert order {order_id}. Error: {str(e)}")

# Close connections
cursor.close()
conn.close()
