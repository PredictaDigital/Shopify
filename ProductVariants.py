import requests
import pyodbc
from datetime import datetime

# Shopify API credentials
SHOP_NAME = 'littlecheeks-com-au.myshopify.com'
API_VERSION = '2024-10'
ACCESS_TOKEN = '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'

# SQL Server database credentials
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
database_table = "dbo.Shopify_Littlecheeks_Refunds_KS"

# Establish connection to SQL Server database
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Truncate the table
cursor.execute(f'TRUNCATE TABLE {database_table}')
conn.commit()

# Shopify API endpoint for orders
orders_url = f'https://{SHOP_NAME}/admin/api/{API_VERSION}/orders.json'
params = {'limit': 250, 'status': 'any'}  # Ensure we get all orders, including closed ones

# Headers for the request
headers = {
    'X-Shopify-Access-Token': ACCESS_TOKEN,
    'Content-Type': 'application/json'
}

refunds = []
next_url = orders_url

while next_url:
    # Make the request to Shopify API
    response = requests.get(next_url, headers=headers, params=params if next_url == orders_url else {})

    # Check the response status code
    if response.status_code == 200:
        orders_data = response.json().get('orders', [])

        # Process the orders data to extract refunds
        for order in orders_data:
            if 'refunds' in order and order['refunds']:
                for refund in order['refunds']:
                    # Check if refund_line_items has data
                    if refund.get('refund_line_items'):
                        # Process each line item in the refund as before
                        for refund_line_item in refund['refund_line_items']:
                            line_item = next((item for item in order.get('line_items', []) if
                                              item['id'] == refund_line_item['line_item_id']), None)
                            if line_item:
                                variant_id = line_item['variant_id']
                                product_id = line_item['product_id']
                                sku = line_item['sku']
                                name = line_item['name']
                                title = line_item['title']
                                quantity = refund_line_item['quantity']
                                total_refunded = refund_line_item.get('subtotal', '0')
                                refunds.append({
                                    'refund_id': refund['id'],
                                    'order_id': order['id'],
                                    'order_number': order['order_number'],
                                    'created_at': refund['created_at'],
                                    'processed_at': refund['processed_at'],
                                    'note': refund.get('note', None),
                                    'quantity': quantity,
                                    'total_refunded': total_refunded,
                                    'variant_id': variant_id,
                                    'product_id': product_id,
                                    'sku': sku,
                                    'name': name,
                                    'title': title
                                })
                    else:
                        # If refund_line_items is empty, use transaction amount
                        transaction_amount = refund.get('transactions', [{}])[0].get('amount', '0')
                        refunds.append({
                            'refund_id': refund['id'],
                            'order_id': order['id'],
                            'order_number': order['order_number'],
                            'created_at': refund['created_at'],
                            'processed_at': refund['processed_at'],
                            'note': refund.get('note', None),
                            'quantity': 0,  # Set to 0 if no line item quantity
                            'total_refunded': transaction_amount,
                            'variant_id': None,
                            'product_id': None,
                            'sku': None,
                            'name': None,
                            'title': None
                        })

        # Check if there is a next page
        next_url = None
        if 'Link' in response.headers:
            links = response.headers['Link'].split(',')
            for link in links:
                if 'rel="next"' in link:
                    next_url = link[link.find('<') + 1:link.find('>')]
                    break

        if not next_url:
            break  # No more pages to fetch

    else:
        print(f"Failed to retrieve orders: {response.status_code}")
        print(response.text)
        break

# Insert refunds data into the database
for refund in refunds:
    cursor.execute(f'''
        INSERT INTO {database_table} (created_at, refund_id, order_id, order_number, product_id, variant_id, sku, name, title,  processed_at, note, quantity, total_refunded)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', datetime.strptime(refund['created_at'], '%Y-%m-%dT%H:%M:%S%z'),refund['refund_id'], refund['order_id'],
                   refund['order_number'], refund['product_id'], refund['variant_id'], refund['sku'],refund['name'],
                   refund['title'], datetime.strptime(refund['processed_at'], '%Y-%m-%dT%H:%M:%S%z'), refund['note'],
                   refund['quantity'], refund['total_refunded'])

conn.commit()
cursor.close()
conn.close()
