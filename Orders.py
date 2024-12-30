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
database_table = "dbo.Shopify_Littlecheeks_Products_KS"

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

            # Extract product-level details and add to the list
            for product in products_data:
                product_info = {
                    'product_id': product['id'],
                    'title': product['title'],
                    'product_type': product.get('product_type', ''),
                    'vendor': product['vendor'],
                    'handle': product['handle'],
                    'created_at': datetime.strptime(product['created_at'], '%Y-%m-%dT%H:%M:%S%z').strftime(
                        '%Y-%m-%d %H:%M:%S'),
                    'updated_at': datetime.strptime(product['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime(
                        '%Y-%m-%d %H:%M:%S'),
                    'published_at': datetime.strptime(product['published_at'], '%Y-%m-%dT%H:%M:%S%z').strftime(
                        '%Y-%m-%d %H:%M:%S') if product['published_at'] else None,
                    'published_scope': product.get('published_scope', ''),
                    'tags': product.get('tags', ''),
                    'status': product.get('status', 'active')
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
                product_id, title, product_type, vendor, handle, 
                created_at, updated_at, published_at, published_scope, tags, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        cursor.execute(insert_query, (
            product['product_id'], product['title'], product['product_type'], product['vendor'], product['handle'],
            product['created_at'], product['updated_at'], product['published_at'], product['published_scope'],
            product['tags'], product['status']
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
