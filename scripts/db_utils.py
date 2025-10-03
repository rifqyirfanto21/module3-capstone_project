from sqlalchemy import create_engine
from config.setting import DB_CONFIG

def get_db_connection():
    """
    Database connection to PostgreSQL
    """
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    database = DB_CONFIG["database"]
    user = DB_CONFIG["user"]
    password = DB_CONFIG["password"]

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")
    return engine

engine = get_db_connection()

def insert_users(users):
    """
    Insert users data into users table
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dim_users (full_name, email, address, phone_number, created_at)
                VALUES (:full_name, :email, :address, :phone_number, :created_at))
            """),
            users
        )

def insert_payment_methods(methods):
    """
    Insert payment methods data into payment methods table
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dim_payment_methods (method_name, provider, created_at)
                VALUES (:method_name, :provider, :created_at)
            """),
            methods
        )

def insert_shipping_methods(shippings):
    """
    Insert shipping methods data into shipping methods table
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dim_shipping_methods (carrier_name, shipping_type, created_at)
                VALUES (:carrier_name, :shipping_type, :created_at)
            """),
            shippings
        )

def insert_products(products):
    """
    Insert products data into products table
    """
    with engine.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO dim_products (product_name, brand, category, currency, price, cost, created_at)
                VALUES (:product_name, :brand, :category, :currency, :price, :cost, :created_at)
            """),
            products
        )