import azure.functions as func
import logging
import json
import requests
import pandas as pd
import gspread
import gspread_dataframe as gd

# Load credentials for Shopify and Google Sheets
def load_credentials():
    credentials_file_path = 'credentials.json'
    with open(credentials_file_path, 'r') as file:
        credentials = json.load(file)
    return credentials

def flatten_data(y, shop_name):
    out = {}
    
    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x
    
    flatten(y)
    out['shop_name'] = shop_name  # Add shop_name to the flattened output
    return out

def process_order_data(orders, shop_name):
    processed_orders = []
    for order in orders:
        # Flatten order data, excluding line_items and fulfillments
        order_data = {k: v for k, v in order.items() if k not in ['line_items', 'fulfillments']}
        order_data = flatten_data(order_data, shop_name)

        # Process order line items
        line_items = order.get('line_items', [])
        if not line_items:
            # If there are no line items, include the order with empty item details
            processed_orders.append({
                **order_data,
                'item_type': 'order',
                'item_id': None,
                'item_title': None,
                'item_quantity': None,
                'item_price': None,
                'fulfillment_id': None,
                'fulfillment_status': None
            })
        else:
            for item in line_items:
                processed_orders.append({
                    **order_data,
                    'item_type': 'order',
                    'item_id': item.get('id'),
                    'item_title': item.get('title'),
                    'item_quantity': item.get('quantity'),
                    'item_price': item.get('price'),
                    'fulfillment_id': None,
                    'fulfillment_status': None
                })
        
        # Process fulfillments and their line items
        fulfillments = order.get('fulfillments', [])
        for fulfillment in fulfillments:
            fulfillment_id = fulfillment.get('id')
            fulfillment_status = fulfillment.get('status')
            fulfillment_items = fulfillment.get('line_items', [])
            
            if not fulfillment_items:
                # If there are no fulfillment items, include the fulfillment with empty item details
                processed_orders.append({
                    **order_data,
                    'item_type': 'fulfillment',
                    'item_id': None,
                    'item_title': None,
                    'item_quantity': None,
                    'item_price': None,
                    'fulfillment_id': fulfillment_id,
                    'fulfillment_status': fulfillment_status
                })
            else:
                for item in fulfillment_items:
                    processed_orders.append({
                        **order_data,
                        'item_type': 'fulfillment',
                        'item_id': item.get('id'),
                        'item_title': item.get('title'),
                        'item_quantity': item.get('quantity'),
                        'item_price': item.get('price'),
                        'fulfillment_id': fulfillment_id,
                        'fulfillment_status': fulfillment_status
                    })
    
    return processed_orders

# Fetch all customers from a Shopify store
def fetch_all_customers_from_shopify(shop_name, access_token, api_version="2024-01"):
    url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/customers.json"
    headers = {"X-Shopify-Access-Token": access_token}
    customers = []
    params = {"limit": 250}
    
    while True:
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            data = response.json()
            customers.extend(data['customers'])
            
            # Check if there's a next page
            link_header = response.headers.get('Link')
            if link_header and 'rel="next"' in link_header:
                next_page_url = [link.split(';')[0].strip('<>') for link in link_header.split(',') if 'rel="next"' in link_header]
                if next_page_url:
                    url = next_page_url[0]  # Update URL for the next page
                else:
                    break
            else:
                break
        else:
            response.raise_for_status()
    
    return customers

def fetch_all_orders_from_shopify(shop_name, access_token, api_version="2024-01"):
    url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}/orders.json"
    headers = {"X-Shopify-Access-Token": access_token}
    orders = []
    params = {"limit": 250, "status": "any"}  # Include all order statuses

    while True:
        logging.info(f"Fetching orders from URL: {url}")
        response = requests.get(url, headers=headers, params=params)
        logging.info(f"Response status code: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            new_orders = data.get('orders', [])
            logging.info(f"Fetched {len(new_orders)} orders")
            orders.extend(new_orders)

            # Check if there's a next page
            link_header = response.headers.get('Link')
            if link_header and 'rel="next"' in link_header:
                next_page_url = [link.split(';')[0].strip('<>') for link in link_header.split(',') if 'rel="next"' in link]
                if next_page_url:
                    url = next_page_url[0]  # Update URL for the next page
                    logging.info(f"Next page URL: {url}")
                else:
                    break
            else:
                logging.info("No more pages to fetch")
                break
        else:
            logging.error(f"Error fetching orders: {response.text}")
            response.raise_for_status()

    logging.info(f"Total orders fetched: {len(orders)}")
    return orders

# Upload customer data to a specific tab in Google Sheets
def upload_to_google_sheets(sheet_name, tab_name, data):
    # Load Google Sheets credentials from the same JSON file
    credentials = load_credentials()
    google_credentials = credentials.get('Google', {})
    
    # Create a credentials object for gspread
    client = gspread.service_account_from_dict(google_credentials)
    
    # Open the Google Sheet
    spreadsheet = client.open(sheet_name)
    
    # Select or create the tab/sheet
    try:
        sheet = spreadsheet.worksheet(tab_name)  # Try to select the existing sheet
    except gspread.exceptions.WorksheetNotFound:
        sheet = spreadsheet.add_worksheet(title=tab_name, rows="1000", cols="26")  # Create a new sheet if not found
    
    # Clear the existing tab data
    sheet.clear()
    
    # Upload data
    gd.set_with_dataframe(worksheet=sheet, dataframe=data, include_index=False, include_column_header=True, resize=True)

# Main function to handle fetching, saving, and uploading for multiple stores
def process_stores():
    try:
        logging.info("Starting process_stores function")
        credentials = load_credentials()
        results = []

        # Initialize lists to accumulate all customers and orders
        all_customers = []
        all_orders = []

        # List of stores and corresponding Google Sheets tabs
        stores = {
            "SHOPIFY_EU": ("Customer_EU", "Orders_EU"),
            "SHOPIFY_CZ": ("Customer_CZ", "Orders_CZ"),
            "SHOPIFY_CARPORT": ("Customer_Carport", "Orders_Carport")
        }
        # Google Sheet name
        google_sheet_name = "Reporting_Shopify"

        for store, (tab_customers, tab_orders) in stores.items():
            logging.info(f"Processing store: {store}")
            shopify_credentials = credentials.get(store, {})
            shop_name = shopify_credentials.get("SHOP_NAME")
            access_token = shopify_credentials.get("API_ACCESS_TOKEN")
            api_version = shopify_credentials.get("API_VERSION", "2024-01")

            if shop_name and access_token:
                logging.info(f"Credentials found for store: {shop_name}")
                
                logging.info(f"Fetching customers for store: {shop_name}")
                customers = fetch_all_customers_from_shopify(shop_name, access_token, api_version)
                
                logging.info(f"Fetching orders for store: {shop_name}")
                orders = fetch_all_orders_from_shopify(shop_name, access_token, api_version)

                if customers:
                    logging.info(f"Processing {len(customers)} customers for {shop_name}")
                    flattened_customers = [flatten_data(customer,shop_name) for customer in customers]
                    df_customers = pd.DataFrame(flattened_customers)
                    upload_to_google_sheets(google_sheet_name, tab_customers, df_customers)
                    all_customers.extend(flattened_customers)
                    results.append(f"Flattened customers data for {shop_name} has been saved to Google Sheets tab: {tab_customers}.")
                else:
                    logging.warning(f"No customer data found for store: {shop_name}")
                    results.append(f"No customer data found for store: {shop_name}")

                if orders:
                    logging.info(f"Processing {len(orders)} orders for {shop_name}")
                    processed_orders = process_order_data(orders,shop_name)
                    df_orders = pd.DataFrame(processed_orders)
                    upload_to_google_sheets(google_sheet_name, tab_orders, df_orders)
                    all_orders.extend(processed_orders)
                    results.append(f"Processed orders data for {shop_name} has been saved to Google Sheets tab: {tab_orders}.")
                else:
                    logging.warning(f"No orders data found for store: {shop_name}")
                    results.append(f"No orders data found for store: {shop_name}")
            else:
                logging.error(f"Missing credentials for store: {store}")
                results.append(f"Missing credentials for store: {store}")

        # Convert lists of dictionaries to DataFrames
        if all_customers:
            df_all_customers = pd.DataFrame(all_customers).add_prefix('customers_')

        if all_orders:
            df_all_orders = pd.DataFrame(all_orders).add_prefix('orders_')

        # Merge the DataFrames on the customer ID, ensuring all orders are retained and customer details are added
        if all_customers and all_orders:
            df_combined = pd.merge(df_all_orders, df_all_customers, left_on='orders_customer_id', right_on='customers_id', how='left')

            # Upload the merged data to Google Sheets
            upload_to_google_sheets(google_sheet_name, "Combined_Customers_Orders", df_combined)
            results.append("Merged customer and orders data has been saved to Google Sheets tab: Combined_Customers_Orders.")

        # Upload individual datasets to Google Sheets if necessary
        if all_customers:
            upload_to_google_sheets(google_sheet_name, "Customers_all", df_all_customers)
            results.append("Combined customer data has been saved to Google Sheets tab: Customers_all.")

        if all_orders:
            upload_to_google_sheets(google_sheet_name, "Orders_all", df_all_orders)
            results.append("Combined orders data has been saved to Google Sheets tab: Orders_all.")
        result_message = "\n".join(results)
        logging.info(f"Process completed. Results: {result_message}")
        return result_message
    except Exception as e:
        logging.error(f"Error in process_stores: {str(e)}")
        raise

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="http_trigger", methods=["GET"])
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        result = process_stores()
        return func.HttpResponse(f"Function executed successfully. Results: {result}", status_code=200)
    except Exception as e:
        logging.error(f"Error in http_trigger: {str(e)}")
        return func.HttpResponse(f"An error occurred: {str(e)}", status_code=500)