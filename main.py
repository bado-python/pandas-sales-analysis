import pandas as pd

df_sales_data_raw = pd.read_csv("sales_data.csv")
df_customers_raw = pd.read_csv("customers.csv")

# 元データを保持するためコピー
df_sales_data = df_sales_data_raw.copy()
df_customers = df_customers_raw.copy()

# customer_idごとに売り上げを集計
df_summary = (
    df_sales_data.groupby("customer_id")
    .agg(
        total_sales=("sales", "sum"),
        avg_sales=("sales", "mean"),
        order_count=("sales", "count"),
    )
    .reset_index()
)

# 集計結果を基準にするため left join を使用
df_merged = pd.merge(df_summary, df_customers, on="customer_id", how="left")

# 結合後の構造確認は info() で実施
df_cleaned = df_merged.sort_values(
    ["total_sales", "order_count"], ascending=(False, False)
).reset_index(drop=True)
df_cleaned = df_cleaned[
    [
        "customer_id",
        "customer_name",
        "region",
        "total_sales",
        "avg_sales",
        "order_count",
    ]
]

# マスタに存在しない顧客は分析対象外とするため除外
df_cleaned = df_cleaned.loc[df_cleaned["customer_name"].notna()].reset_index(drop=True)

df_cleaned.to_csv("final_result.csv", index=False)
