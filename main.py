import pandas as pd
import numpy as np

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

# マスタに存在しない顧客は分析対象外とするため除外
df_cleaned = df_cleaned.loc[df_cleaned["customer_name"].notna()].reset_index(drop=True)

# 顧客ランク付け
conditions = [
    df_cleaned["total_sales"] >= 3000,
    df_cleaned["total_sales"] >= 2000,
    df_cleaned["total_sales"] >= 1000,
]
choices = ["S", "A", "B"]
df_cleaned["customer_rank"] = np.select(conditions, choices, default="C")
df_cleaned["is_priority_customer"] = df_cleaned["customer_rank"].isin(["S", "A"])

df_cleaned = df_cleaned[
    [
        "customer_id",
        "customer_name",
        "region",
        "total_sales",
        "avg_sales",
        "order_count",
        "customer_rank",
        "is_priority_customer",
    ]
]

df_cleaned.to_csv("final_result.csv", index=False)
