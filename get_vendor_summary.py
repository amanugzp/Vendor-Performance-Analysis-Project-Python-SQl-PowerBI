import sqlite3
import pandas as pd  
import logging
from ingestion_db import ingest_db  # ✅ Assuming this ingests DataFrame to DB

# Logging Configuration
logging.basicConfig(
    filename=r"C:\Data_Science\Vendor Performance Analysis Project (Python+SQl+PowerBI)\logs\get_vendor_summary.log",  # ✅ Raw string path and typo fixed
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

def create_vendor_summary(conn):
    """This function merges different tables to create the overall vendor summary."""
    query = """
    WITH FreightSummary AS (
        SELECT
            VendorNumber,
            SUM(Freight) AS FreightCost
        FROM vendor_invoice
        GROUP BY VendorNumber
    ),
    PurchaseSummary AS (
        SELECT
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Volume,
            pp.Price AS ActualPrice,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY 
            p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Volume, pp.Price
    ),
    SalesSummary AS (
        SELECT
            VendorNo,
            Brand,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.Volume,
        ps.ActualPrice,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalSalesQuantity,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss
        ON ps.VendorNumber = ss.VendorNo AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC
    """
    return pd.read_sql_query(query, conn)


def clean_data(df):
    """This function cleans the data and adds new derived columns."""
    
    # ✅ Corrected method typo: fillna, not filllna
    df['Volume'] = pd.to_numeric(df['Volume'], errors='coerce')  # Safer conversion
    df.fillna(0, inplace=True)

    # ✅ Strip whitespace
    df['VendorName'] = df['VendorName'].astype(str).str.strip()
    df['Description'] = df['Description'].astype(str).str.strip()

    # ✅ Create new KPIs
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = df.apply(
        lambda row: (row['GrossProfit'] / row['TotalSalesDollars']) * 100 if row['TotalSalesDollars'] else 0,
        axis=1
    )
    df['StockTurnover'] = df.apply(
        lambda row: row['TotalSalesQuantity'] / row['TotalPurchaseQuantity'] if row['TotalPurchaseQuantity'] else 0,
        axis=1
    )
    df['SalestoPurchaseRatio'] = df.apply(
        lambda row: row['TotalSalesDollars'] / row['TotalPurchaseDollars'] if row['TotalPurchaseDollars'] else 0,
        axis=1
    )

    return df


if __name__ == '__main__':
    conn = sqlite3.connect('inventory.db')

    logging.info("Creating Vendor Summary Table.......")
    summary_df = create_vendor_summary(conn)
    logging.info(summary_df.head().to_string())

    logging.info('Cleaning Data....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head().to_string())

    logging.info("Ingesting data into DB.......")
    ingest_db(clean_df, "create_vendor_summary", conn)  # ✅ Pass the cleaned DataFrame and table name
    logging.info('Ingestion Completed')
