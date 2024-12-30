import requests
import pyodbc
from datetime import datetime

# Shopify API credentials
shop_name = 'littlecheeks-com-au.myshopify.com'
api_version = '2024-04'
access_token = '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'

# Establish a connection to your SQL Server database
server = 'Predicta.Database.Windows.Net'
database = 'Predicta'
username = 'PredictaAdmin'
password = 'Yhf^43*&^FHHytf'
combined_collections_table = "dbo.Shopify_Littlecheeks_Collections_KS"

conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')
cursor = conn.cursor()

# Truncate the combined table
cursor.execute(f'TRUNCATE TABLE {combined_collections_table}')


def fetch_data(endpoint, key):
    collected_data = []
    next_page_cursor = None

    while True:
        params = {'limit': 250}
        headers = {
            'X-Shopify-Access-Token': access_token,
            'Content-Type': 'application/json'
        }

        if next_page_cursor:
            params['page_info'] = next_page_cursor

        response = requests.get(endpoint, params=params, headers=headers)

        if response.status_code == 200:
            data = response.json().get(key, [])
            collected_data.extend(data)

            if 'link' in response.headers:
                links = response.headers['link'].split(',')
                next_link = [link for link in links if 'rel="next"' in link]
                if next_link:
                    next_page_cursor = next_link[0].split(';')[0].strip('<>').split('page_info=')[-1]
                else:
                    break
            else:
                break
        else:
            print(f"Failed to fetch data from {endpoint}: {response.status_code} - {response.text}")
            break

    return collected_data


# Fetch collections data
custom_collections = fetch_data(f'https://{shop_name}/admin/api/{api_version}/custom_collections.json',
                                'custom_collections')
smart_collections = fetch_data(f'https://{shop_name}/admin/api/{api_version}/smart_collections.json',
                               'smart_collections')

# Fetch collects data
collects = fetch_data(f'https://{shop_name}/admin/api/{api_version}/collects.json', 'collects')

# Fetch products data
products = fetch_data(f'https://{shop_name}/admin/api/{api_version}/products.json', 'products')

# Create a dictionary for products with their variants
product_variants = {}
for product in products:
    product_id = product['id']
    product_title = product['title']
    for variant in product.get('variants', []):
        variant_id = variant['id']
        variant_title = variant['title']
        sku = variant.get('sku')
        product_variants[variant_id] = {
            'product_id': product_id,
            'product_name': product_title,
            'variant_id': variant_id,
            'variant_title': variant_title,
            'sku': sku,
        }


# Insert collection and product variant data into the database
def insert_collection_data(collections, collection_type):
    for collection in collections:
        collection_id = collection['id']
        handle = collection['handle']
        title = collection['title']
        updated_at = datetime.strptime(collection['updated_at'], '%Y-%m-%dT%H:%M:%S%z').strftime('%Y-%m-%d %H:%M:%S')
        body_html = collection.get('body_html')
        published_at = datetime.strptime(collection['published_at'], '%Y-%m-%dT%H:%M:%S%z').strftime(
            '%Y-%m-%d %H:%M:%S') if collection.get('published_at') else None
        sort_order = collection.get('sort_order')
        template_suffix = collection.get('template_suffix')
        published_scope = collection['published_scope']
        admin_graphql_api_id = collection['admin_graphql_api_id']
        disjunctive = collection.get('disjunctive') if collection_type == 'smart' else None
        rules = str(collection.get('rules')) if collection_type == 'smart' else None

        # Find product variants associated with this collection
        associated_product_ids = set()

        if collection_type == 'custom':
            for collect in collects:
                if collect['collection_id'] == collection_id:
                    associated_product_ids.add(collect['product_id'])
        else:
            for product in products:
                # Evaluate product against smart collection rules
                # Here, you can add rule evaluation logic if necessary
                associated_product_ids.add(product['id'])

        for product_id in associated_product_ids:
            for variant_id, variant_info in product_variants.items():
                if variant_info['product_id'] == product_id:
                    variant_title = variant_info['variant_title']
                    sku = variant_info['sku']

                    # Define the INSERT query
                    insert_query = (
                        f"INSERT INTO {combined_collections_table} (collection_id, handle, title, updated_at, body_html, "
                        f"published_at, sort_order, template_suffix, published_scope, admin_graphql_api_id, disjunctive, "
                        f"rules, collection_type, product_id, product_name, variant_id, variant_title, sku) "
                        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    )

                    # Execute the insert query
                    cursor.execute(insert_query, (
                        collection_id, handle, title, updated_at, body_html, published_at, sort_order, template_suffix,
                        published_scope, admin_graphql_api_id, disjunctive, rules, collection_type, product_id,
                        variant_info['product_name'], variant_id, variant_title, sku
                    ))


# Process and insert custom collections
insert_collection_data(custom_collections, 'custom')

# Process and insert smart collections
insert_collection_data(smart_collections, 'smart')

# Commit all changes
conn.commit()

# Close connections
cursor.close()
conn.close()
