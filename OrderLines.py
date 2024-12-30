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
database_table = "dbo.Shopify_Littlecheeks_Orders_KS"

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
    params = {        "limit": 250,  # max limit per request
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

# Iterate through orders_data and insert each order into SQL
for order in orders_data:
    try:
        order_id = order['id']
        order_name = order['name']
        admin_graphql_api_id = order['admin_graphql_api_id']
        order_number = order['order_number']
        created_at = datetime.strptime(order['created_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
        updated_at = datetime.strptime(order['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')

        if order['customer']:
            customer_id = order['customer']['id']
            customer_first_name = order['customer']['first_name']
            customer_last_name = order['customer']['last_name']
            email = order['customer']['email']
        else:
            customer_id = None
            customer_first_name = None
            customer_last_name = None
            email = None

        currency = order['currency']
        cancel_reason = order['cancel_reason']
        cancelled_at = order['cancelled_at']

        if cancelled_at and cancelled_at.lower() != 'null':
            cancelled_at = datetime.strptime(cancelled_at, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
        else:
            cancelled_at = None

        cart_token = order['cart_token']
        checkout_id = order['checkout_id']
        checkout_token = order['checkout_token']
        confirmation_number = order['confirmation_number']
        confirmed = order['confirmed']
        financial_status = order['financial_status']
        fulfillment_status = order['fulfillment_status']
        subtotal_price = order.get('total_line_items_price', '0')
        final_subtotal_price = order.get('subtotal_price', '0')
        total_discounts = order.get('total_discounts', '0')
        total_tax = order.get('total_tax', '0')
        total_shipping = order.get('total_shipping_price_set', {}).get('shop_money', {}).get('amount', '0')
        total_order_price = order.get('total_price', '0')
        processed_at = order['processed_at']

        if processed_at and processed_at.lower() != 'null':
            processed_at = datetime.strptime(processed_at, '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
        else:
            processed_at = None

        if order['billing_address']:
            billing = order['billing_address']
            billing_address = f"{billing.get('address1', '')} {billing.get('address2', '')}"
            billing_city = billing.get('city', '')
            billing_province = billing.get('province', '')
            billing_zip = billing.get('zip', '')
            billing_country = billing.get('country', '')
        else:
            billing_address = None
            billing_city = None
            billing_province = None
            billing_zip = None
            billing_country = None

        if order['shipping_address']:
            shipping = order['shipping_address']
            shipping_address = f"{shipping.get('address1', '')} {shipping.get('address2', '')}"
            shipping_city = shipping.get('city', '')
            shipping_province = shipping.get('province', '')
            shipping_zip = shipping.get('zip', '')
            shipping_country = shipping.get('country', '')
        else:
            shipping_address = None
            shipping_city = None
            shipping_province = None
            shipping_zip = None
            shipping_country = None

        total_weight = order.get('total_weight', '0')

        # Iterate through line items
        for item in order['line_items']:
            lineitem_quantity = item.get('quantity', 0)
            lineitem_name = item.get('name', '')
            lineitem_price = item.get('price', '0')
            lineitem_sku = item.get('sku', '')
            lineitem_fulfillment_status = item.get('fulfillment_status', '')

            # Define the INSERT query for orders and line items
            insert_query = (
                f"INSERT INTO {database_table} (order_id, order_name, admin_graphql_api_id, order_number, created_at, "
                f"customer_id, customer_first_name, customer_last_name, email, currency, lineitem_sku, lineitem_name, "
                f"lineitem_quantity, lineitem_price, subtotal_price, total_discounts, final_subtotal_price, total_tax, "
                f"total_shipping, total_order_price, lineitem_fulfillment_status, cancel_reason, cancelled_at, cart_token, "
                f"checkout_id, checkout_token, confirmation_number, confirmed, financial_status, fulfillment_status, "
                f"updated_at, processed_at, billing_address, billing_city, billing_province, billing_zip, billing_country, "
                f"shipping_address, shipping_city, shipping_province, shipping_zip, shipping_country, total_weight) "
                f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                f"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )

            # Execute the query for each line item
            cursor.execute(insert_query, (order_id, order_name, admin_graphql_api_id, order_number, created_at,
                                          customer_id, customer_first_name, customer_last_name, email, currency,
                                          lineitem_sku, lineitem_name, lineitem_quantity, lineitem_price, subtotal_price,
                                          total_discounts, final_subtotal_price, total_tax, total_shipping,
                                          total_order_price, lineitem_fulfillment_status, cancel_reason, cancelled_at,
                                          cart_token, checkout_id, checkout_token, confirmation_number, confirmed,
                                          financial_status, fulfillment_status, updated_at, processed_at, billing_address, billing_city,
                                          billing_province, billing_zip, billing_country, shipping_address, shipping_city,
                                          shipping_province, shipping_zip, shipping_country, total_weight))
        conn.commit()

    except Exception as e:
        print(f"Failed to insert order {order_id}. Error: {str(e)}")

# Close connections
cursor.close()
conn.close()
