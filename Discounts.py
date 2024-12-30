import requests
import pyodbc
from datetime import datetime

# Shopify API credentials
shop_name = 'littlecheeks-com-au.myshopify.com'
api_version = '2024-04'
access_token = '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'

# SQL Server database credentials
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
inventory_table = "dbo.Shopify_Littlecheeks_Inventory_KS"

# Establish a connection to your SQL Server database
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Truncate the table before inserting new data
cursor.execute(f'TRUNCATE TABLE {inventory_table}')
conn.commit()

# Function to fetch all products and their variants using cursor-based pagination
def fetch_inventory_item_ids():
    products_endpoint = f'https://{shop_name}/admin/api/{api_version}/products.json'
    headers = {
        'X-Shopify-Access-Token': access_token,
        'Content-Type': 'application/json'
    }

    inventory_item_ids = []
    product_variants = {}
    next_page_url = products_endpoint  # Initial endpoint to start fetching products

    while next_page_url:
        response = requests.get(next_page_url, headers=headers, params={'limit': 250})

        if response.status_code == 200:
            products_data = response.json().get('products', [])

            for product in products_data:
                product_id = product['id']
                for variant in product.get('variants', []):
                    inventory_item_id = variant['inventory_item_id']
                    variant_id = variant['id']
                    if inventory_item_id not in inventory_item_ids:
                        inventory_item_ids.append(inventory_item_id)
                        product_variants[inventory_item_id] = (product_id, variant_id)

            # Check for pagination in the headers
            next_page_url = None
            if 'Link' in response.headers:
                links = response.headers['Link'].split(',')
                for link in links:
                    if 'rel="next"' in link:
                        next_page_url = link[link.find("<") + 1:link.find(">")]
                        break
        else:
            print(f"Failed to fetch products: {response.status_code} - {response.text}")
            break

    return inventory_item_ids, product_variants


# Function to fetch and insert inventory items into SQL Server
def load_inventory_data_to_sql(inventory_item_ids, product_variants):
    inventory_items_endpoint = f'https://{shop_name}/admin/api/{api_version}/inventory_items.json'
    headers = {
        'X-Shopify-Access-Token': access_token,
        'Content-Type': 'application/json'
    }

    processed_inventory_item_ids = set()  # Set to track processed IDs

    batch_size = 50
    for i in range(0, len(inventory_item_ids), batch_size):
        batch_ids = inventory_item_ids[i:i + batch_size]
        params = {'ids': ','.join(map(str, batch_ids))}

        response = requests.get(inventory_items_endpoint, headers=headers, params=params)

        if response.status_code == 200:
            inventory_items_data = response.json().get('inventory_items', [])

            for item in inventory_items_data:
                inventory_item_id = item['id']

                # Skip if already processed
                if inventory_item_id in processed_inventory_item_ids:
                    continue

                processed_inventory_item_ids.add(inventory_item_id)  # Mark as processed

                sku = item['sku']
                tracked = item['tracked']
                cost_per_unit = item['cost']
                country_code_of_origin = item.get('country_code_of_origin', '')
                province_code_of_origin = item.get('province_code_of_origin', '')
                created_at = datetime.strptime(item['created_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
                updated_at = datetime.strptime(item['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
                requires_shipping = item['requires_shipping']

                product_id, variant_id = product_variants.get(inventory_item_id, (None, None))

                # Insert the inventory data into SQL Server
                insert_query = f'''
                    INSERT INTO {inventory_table} (
                        inventory_item_id, product_id, variant_id, sku,
                        created_at, updated_at, cost_per_unit, country_code_of_origin,
                        province_code_of_origin, tracked, requires_shipping
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                '''

                cursor.execute(insert_query, (
                    inventory_item_id, product_id, variant_id, sku,
                    created_at, updated_at, cost_per_unit, country_code_of_origin,
                    province_code_of_origin, tracked, requires_shipping
                ))

        else:
            print(f"Failed to fetch inventory items: {response.status_code} - {response.text}")
            break

        # Commit the changes to the database
        conn.commit()


# Fetch inventory data and insert it into SQL Server
inventory_item_ids, product_variants = fetch_inventory_item_ids()
load_inventory_data_to_sql(inventory_item_ids, product_variants)

cursor.close()
conn.close()
