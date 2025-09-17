# import pandas as pd
# import numpy as np
# df = pd.read_csv("online_retail_II.csv")

# # df1 = df.dropna(subset=["Customer ID"])
# # df1 = df1[(df1['Quantity'] > 0) & (df1['Price'] > 0)]
# # df1['Description'] = df1['Description'].astype('string').fillna('UNKNOWN').str.strip().str.upper()
# # df1['InvoiceDate'] = pd.to_datetime(df1['InvoiceDate'])

# df2 = df.copy()
# df2 = df2[(df2['Quantity'] > 0) & (df2['Price'] > 0)]
# df2['Description'] = df2['Description'].astype('string').fillna("UNKNOWN").str.strip().str.upper()
# missing_mask = df2['Customer ID'].isna()
# neg_ids = [-i for i in range(1, missing_mask.sum() + 1)]
# df2.loc[missing_mask,"Customer ID"] = neg_ids
# df2['Customer ID'] = df2['Customer ID'].astype(int)

# df_base = df2.copy()
# df_test = df_base[["Customer ID","Country"]].drop_duplicates(subset=["Customer ID"])

# clients = df_test
# clients.columns = ['code_client','pays_client']
# clients.insert(0, 'id_client', range(1, len(clients) + 1))

# table = {}
# for key, value in zip(clients["code_client"], clients["id_client"]):
#     table[key] = value

# # clients.to_csv("clients.csv", index=False)

# produits = df_base[['StockCode','Description','Price']].drop_duplicates()
# produits.columns = ['code_prod','description','prix_uni']
# produits.insert(0, 'id_prod', range(1, len(produits) + 1))


# prodtable = {}
# for _, row in produits.iterrows():
#     key = (row['code_prod'], row['description'], row['prix_uni'])
#     prodtable[key] = row['id_prod']
# # produits.to_csv("produits.csv",index=False)

# transactions = df_base[['Invoice','InvoiceDate']].drop_duplicates(subset=["Invoice"])
# df_test = df_base[['Invoice','Customer ID']].drop_duplicates(subset=["Invoice"])

# li_id_cl = []
# for key in df_test["Customer ID"]:
#     li_id_cl.append(table[key])

# transactions.columns = ['code_trans','date_trans']
# transactions.insert(0, 'id_trans', range(1, len(transactions) + 1))
# transactions['id_client'] = li_id_cl

# trantable = {}
# for key, value in zip(transactions["code_trans"], transactions["id_trans"]):
#     trantable[key] = value

# # transactions.to_csv("transactions.csv",index=False)

# transprod = df_base[['Invoice','Quantity','StockCode','Description','Price']].copy()

# # Map product IDs
# transprod['id_prod'] = transprod.apply(
#     lambda x: prodtable[(x['StockCode'], x['Description'], x['Price'])],
#     axis=1
# )
# # Map transaction IDs
# transprod['id_trans'] = transprod['Invoice'].map(trantable)

# # Drop unnecessary columns
# transprod.drop(columns=['Invoice', 'StockCode', 'Description', 'Price'], inplace=True)

# # Rename and reorder
# transprod.rename(columns={'Quantity': 'quantite'}, inplace=True)
# transprod = transprod[['id_trans', 'id_prod', 'quantite']]
# transprod = transprod.drop_duplicates(subset=['id_trans', 'id_prod'])
# transprod.to_csv("trans_prod.csv",index=False)

import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# === CONFIGURE THESE ===
DB_USER = "root"
DB_PASS = ""
DB_HOST = "localhost"
DB_NAME = "basedonnee"
TABLE_NAME = "trans_prod"
CSV_FILE = "trans_prod.csv"
CHUNKSIZE = 50000  # adjust if needed (50k rows at a time)

# === CONNECT TO MYSQL ===
engine = create_engine(f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}")

# === LOAD AND INSERT IN CHUNKS ===
for i, chunk in enumerate(pd.read_csv(CSV_FILE, chunksize=CHUNKSIZE)):
    chunk.to_sql(TABLE_NAME, con=engine, if_exists="append", index=False)
    print(f"Inserted chunk {i+1} with {len(chunk)} rows")

print("✅ Done! All data imported.")
