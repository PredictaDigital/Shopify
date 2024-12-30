import requests
import pyodbc
from datetime import datetime

# Shopify API credentials
SHOP_NAME = 'littlecheeks-com-au.myshopify.com'
API_VERSION = '2024-07'
ACCESS_TOKEN = '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'

# SQL Server database credentials
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
database_table = "dbo.Shopify_Littlecheeks_ProductVariants_KS"

# Establish a connection to your SQL Server database
conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Truncate the table before inserting new data
cursor.execute(f'TRUNCATE TABLE {database_table}')
conn.commit()

# Shopify API endpoint for products
products_url = f'https://{SHOP_NAME}/admin/api/{API_VERSION}/products.json'
headers = {
    'X-Shopify-Access-Token': ACCESS_TOKEN,
    'Content-Type': 'application/json'
}


# Function to fetch products with pagination
def fetch_all_products():
    products_info = []  # List to store product details
    next_page_url = products_url  # Start with the initial URL
    params = {'limit': 250}  # Limit to 250 products per page

    while next_page_url:
        response = requests.get(next_page_url, headers=headers, params=params)

        if response.status_code == 200:
            products_data = response.json().get('products', [])

            # Extract product and variant details and add to the list
            for product in products_data:
                for variant in product.get('variants', []):
                    product_info = {
                        'Product_id': product['id'],
                        'title': product['title'],
                        'variant_id': variant['id'],
                        'sku': variant.get('sku', ''),
                        'variant_title': variant.get('title', ''),
                        'variant_price': variant.get('price', 0),
                        'handle': product.get('handle', ''),
                        'created_at': datetime.strptime(product['created_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.strptime(product['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S'),
                        'option1': variant.get('option1', ''),
                        'option2': variant.get('option2', ''),
                        'option3': variant.get('option3', ''),
                        'position': variant.get('position', ''),
                        'inventory_policy': variant.get('inventory_policy', ''),
                        'compare_at_price': variant.get('compare_at_price', 0),
                        'fulfillment_service': variant.get('fulfillment_service', ''),
                        'taxable': variant.get('taxable', False),
                        'grams': variant.get('grams', 0),
                        'weight': variant.get('weight', 0),
                        'weight_unit': variant.get('weight_unit', ''),
                        'requires_shipping': variant.get('requires_shipping', False),
                        'inventory_management': variant.get('inventory_management', ''),
                        'inventory_item_id': variant.get('inventory_item_id', ''),
                        'inventory_quantity': variant.get('inventory_quantity', 0),
                        'old_inventory_quantity': variant.get('old_inventory_quantity', 0)
                    }
                    products_info.append(product_info)

            # Handle pagination using the 'Link' header
            next_page_url = None
            link_header = response.headers.get('Link')
            if link_header:
                # Parse 'Link' header for the "next" URL
                links = link_header.split(',')
                for link in links:
                    if 'rel="next"' in link:
                        next_page_url = link[link.find('<') + 1:link.find('>')]
        else:
            print(f"Failed to fetch data: {response.status_code} - {response.text}")
            break

    return products_info


# Function to load product data into SQL Server
def load_data_to_sql(products_info):
    for product in products_info:
        insert_query = f'''
            INSERT INTO {database_table} (
                product_id, title, variant_id, sku, variant_title, variant_price, handle, created_at, 
                updated_at, option1, option2, option3, position, inventory_policy, compare_at_price,
                fulfillment_service, taxable, grams, weight, weight_unit, requires_shipping, 
                inventory_management, inventory_item_id, inventory_quantity, old_inventory_quantity
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(insert_query, (
            product['Product_id'], product['title'], product['variant_id'], product['sku'], product['variant_title'],
            product['variant_price'], product['handle'], product['created_at'], product['updated_at'],
            product['option1'], product['option2'], product['option3'], product['position'], product['inventory_policy'],
            product['compare_at_price'], product['fulfillment_service'], product['taxable'], product['grams'],
            product['weight'], product['weight_unit'], product['requires_shipping'], product['inventory_management'],
            product['inventory_item_id'], product['inventory_quantity'], product['old_inventory_quantity']
        ))

    # Commit the changes to the database
    conn.commit()


# Fetch all product details
all_products = fetch_all_products()

# Load product data into SQL Server
load_data_to_sql(all_products)

# Close connections
cursor.close()
conn.close()