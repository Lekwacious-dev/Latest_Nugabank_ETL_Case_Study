# import the necessary Libraries
from pyspark.sql import SparkSession
from pyspark.sql import DataFrameWriter
from pyspark.sql.functions import monotonically_increasing_id
import os
import psycopg2

# set java home
os.environ['JAVA_HOME'] = 'C:\java8'


# INITIALISE MY SPARK SESSION
spark = SparkSession.builder \
        .appName("Nuga Bank ETL") \
        .master("local[*]") \
        .config(
            "spark.jars", 
            r"C:\Users\USER\Desktop\Data_Engineering\pyspark\Latest_NugaBank_ETL_Case_Study\postgresql-42.7.8.jar") \
        .getOrCreate()
        

# Extract this historical data into spark dataframe
df = spark.read.csv(r'C:\Users\USER\Desktop\Data_Engineering\pyspark\Latest_NugaBank_ETL_Case_Study\dataset\raw\ziko_logistics_data.csv', header=True, inferSchema=True)


# fill up the missing values
df_clean = df.fillna({
    'Unit_Price' : 0.0,
    'Total_Cost' : 0.0,
    'Discount_Rate' : 0.0,
    'Return_Reason' : 'Unknown'
})


# DATA TRANSFORMATION

# Customer Table
customer = df_clean.select('Region', 'Country', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address') \
                    .withColumn('Customer_ID', monotonically_increasing_id()) \
                    .select('Customer_ID', 'Region', 'Country', 'Customer_Name', 'Customer_Phone', 'Customer_Email', 'Customer_Address')


# Shipping Table
shipping = df_clean.select('Warehouse_Code', 'Ship_Mode', 'Delivery_Status', 'Customer_Satisfaction', 'Item_Returned', 'Return_Reason') \
                    .withColumn('Shipping_ID', monotonically_increasing_id()) \
                    .select('Shipping_ID', 'Warehouse_Code', 'Ship_Mode', 'Delivery_Status', 'Customer_Satisfaction', 'Item_Returned', 'Return_Reason')


# Product Table
product = df_clean.select('Product_List_Title') \
                    .withColumn('Product_ID', monotonically_increasing_id()) \
                    .select('Product_ID', 'Product_List_Title')


transaction = df_clean.select('Date', 'Quantity', 'Unit_Price', 'Total_Cost', 'Discount_Rate', 'Sales_Channel') \
                        .withColumn('Transaction_ID', monotonically_increasing_id()) \
                        .select('Transaction_ID', 'Date','Quantity', 'Unit_Price', 'Total_Cost', 'Discount_Rate', 'Sales_Channel')


# # Transaction fact table
df = df_clean.alias("df")
c  = customer.alias("c")
p  = product.alias("p")
s  = shipping.alias("s")

transaction_fact = (
    df
    .join(c, df["Customer_ID"] == c["Customer_ID"], "left")
    .join(p, df["Product_ID"] == p["Product_ID"], "left")

    .select(
        [df[col] for col in ["Transaction_ID", "Customer_ID", "Product_ID", 
                             "Date", "Quantity", "Unit_Price", "Total_Cost", "Discount_Rate", "Sales_Channel"]]
    )
)


from pyspark.sql.functions import col

transaction_fact_for_db = transaction_fact.select(
    col("Transaction_ID").alias("transaction_id"),
    col("Customer_ID").alias("customer_id"),
    col("Product_ID").alias("product_id"),
    col("Date"),
    col("Quantity").alias("quantity"),
    col("Unit_Price").alias("unit_price"),
    col("Total_Cost").alias("total_cost"),
    col("Discount_Rate").alias("discount_rate"),
    col("Sales_Channel").alias("sales_channel")
)



# DATA LOADING

# Connection
def get_db_connection():
    connection = psycopg2.connect(
        host='localhost',
        database = 'nuga_bank_db',
        user= 'postgres',
        password='password'
    )
    return connection


# Create a function to create tables
def create_tables():
    conn = get_db_connection()
    cursor = conn.cursor()
    create_table_query = '''
                        DROP TABLE IF EXISTS customer;
                        DROP TABLE IF EXISTS product;
                        DROP TABLE IF EXISTS shipping;
                        DROP TABLE IF EXISTS transaction_fact;

                        CREATE TABLE customer (
                            Customer_id BIGINT PRIMARY KEY,
                            Region VARCHAR(100),
                            Country VARCHAR(100),
                            Customer_Name VARCHAR(200),
                            Customer_Phone VARCHAR(50),
                            Customer_Email VARCHAR(200),
                            Customer_Address VARCHAR(1000)

                        );

                        CREATE TABLE product (
                            product_id BIGINT PRIMARY KEY,
                            Product_List_Title VARCHAR(1000)
                        );

                        CREATE TABLE shipping (
                            shipping_id BIGINT PRIMARY KEY,
                            Warehouse_Code VARCHAR(200),
                            Ship_Mode VARCHAR (200),
                            Delivery_Status VARCHAR(200),
                            Customer_Satisfaction VARCHAR(200),
                            Item_Returned BOOLEAN,
                            Return_Reason TEXT

                        );

                        CREATE TABLE transaction_fact(
                            Transaction_ID BIGINT PRIMARY KEY,
                            Customer_ID BIGINT NOT NULL,
                            Product_ID BIGINT NOT NULL,
                            Date Timestamp,
                            Quantity INT,
                            Unit_Price NUMERIC(12,2),
                            Total_Cost NUMERIC(14,2),
                            Discount_Rate NUMERIC(5,4),
                            Sales_Channel VARCHAR(1000)


                        );


                        '''
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()
    conn.close()

create_tables()


# LOAD DATA TO TABLE
url = "jdbc:postgresql://localhost:5432/nuga_bank_db"
properties = {
    "user" : "postgres",
    "password" : "password",
    "driver" : "org.postgresql.Driver"
}


customer.write.jdbc(url=url, table="customer", mode="append",  properties=properties)
product.write.jdbc(url=url, table="product", mode="append", properties=properties)
shipping.write.jdbc(url=url, table="shipping", mode="append", properties=properties)
transaction_fact_for_db.write.jdbc(url=url, table="transaction_fact", mode="append", properties=properties)

