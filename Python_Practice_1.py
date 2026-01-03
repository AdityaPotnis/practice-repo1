# # csv file has following columns : Date, Product_ID, Quantity_Sold, and Revenue.
# # Load csv file in pandas dataframe
# # calculate total revenue for each product
# # identify product with highest total revenue
# # Filter products with product_id 100 and 200
# import pandas as pd

# # 1. Load csv file in pandas dataframe
# df = pd.read_csv('sales_data.csv')  # Replace 'your_file.csv' with your actual file name

# # 2. Calculate total revenue for each product
# total_revenue = df.groupby('Product_ID')['Revenue'].sum()
# print( f"Total Revenue for each product: {total_revenue}")


# # 3. Identify product with highest total revenue
# highest_revenue_product = total_revenue.idxmax()
# highest_revenue_value = total_revenue.max()
# print(f"Product with highest revenue: {highest_revenue_product} (Revenue: {highest_revenue_value})")

# # 4. Filter products with product_id 100 and 200
# filtered_products = df[~df['Product_ID'].isin([100, 200])]
# print(filtered_products.shape[0])

# # 5. display total quantity in each month
# tot_qty_prod = df.groupby('Product_ID')['Quantity_Sold'].sum()
# print(f"Total quantity sold for each product: {tot_qty_prod.max()}")

# # 6. Date operations and total qty as month
# df['Date'] = pd.to_datetime(df['Date'])  # Ensure 'Date' column is in datetime format
# tot_qty_mnth = df.groupby(df['Date'].dt.to_period('M'))['Quantity_Sold'].sum()
# tot_qty_mnth1 = df.groupby(df['Date'].dt.month)['Quantity_Sold'].sum()
# print(f"Total quantity sold per month:\n{tot_qty_mnth}\ntot_qty_mnth1:\n{tot_qty_mnth1}")

# # 7. Join dataframes
# df1 = pd.read_csv('product_data.csv')  
# joined_df = pd.merge(df, df1, on='Product_ID', how='inner')  
# prod_wise_revenue = joined_df.groupby('Product_Name')['Revenue'].sum()
# print(prod_wise_revenue)
# # print(len(joined_df))
# # print(len(df))
# # print(len(df1))

# # 8. remove duplicates in dataframe
# joined_df1 = joined_df.drop_duplicates()
# print(len(df[df['Product_ID'] == 100]))
# print(len(df1[df1['Product_ID'] == 100]))
# print(len(joined_df[joined_df['Product_ID'] == 100]))
# print(joined_df1[joined_df1['Product_ID'] == 100].shape[0])


import datetime


today_str = datetime.datetime.now().strftime("%Y%m%d")
print(f"nifty_prediction_{today_str}")
