import requests
import pyodbc
from datetime import datetime

# Shopify API credentials
shop_name = 'littlecheeks-com-au.myshopify.com'
api_version = '2024-10'
access_token = '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'

# SQL Server database credentials
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
inventory_levels_table = "dbo.Shopify_Littlecheeks_InventoryLevels_KS"

# Establish a connection to your SQL Server database
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Truncate the table (optional: if you want to clear the table before inserting new data)
cursor.execute(f'TRUNCATE TABLE {inventory_levels_table}')

# Headers for Shopify API requests
headers = {
    'X-Shopify-Access-Token': access_token,
    'Content-Type': 'application/json'
}

# Step 1: Fetch products to get variant information
products_endpoint = f'https://{shop_name}/admin/api/{api_version}/products.json'
inventory_item_ids = []

response = requests.get(products_endpoint, headers=headers, params={'limit': 250})
if response.status_code == 200:
    products_data = response.json().get('products', [])
    for product in products_data:
        for variant in product.get('variants', []):
            inventory_item_ids.append(variant['inventory_item_id'])
else:
    print(f"Failed to fetch products: {response.status_code} - {response.text}")

# Step 2: Fetch inventory levels using `inventory_item_ids`
inventory_levels_endpoint = f'https://{shop_name}/admin/api/{api_version}/inventory_levels.json'

# Process in batches to avoid API limits
for i in range(0, len(inventory_item_ids), 50):
    batch_ids = inventory_item_ids[i:i + 50]
    params = {'inventory_item_ids': ','.join(map(str, batch_ids))}

    response = requests.get(inventory_levels_endpoint, headers=headers, params=params)

    if response.status_code == 200:
        inventory_levels_data = response.json().get('inventory_levels', [])
        # Insert inventory levels data into the database
        for inventory_level in inventory_levels_data:
            updated_at = datetime.strptime(inventory_level['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime(
                '%Y-%m-%d %H:%M:%S')
            cursor.execute(f'''
                INSERT INTO {inventory_levels_table} (inventory_item_id, location_id, available, updated_at)
                VALUES (?, ?, ?, ?)
            ''', inventory_level['inventory_item_id'], inventory_level['location_id'], inventory_level.get('available'),
                           updated_at)

        # Commit the transaction after each batch
        conn.commit()

    else:
        print(f"Failed to fetch inventory levels: {response.status_code} - {response.text}")

# Close connections
cursor.close()
conn.close()
