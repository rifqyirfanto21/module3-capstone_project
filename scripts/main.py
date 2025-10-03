from generate_data import (
    generate_users,
    generate_payment_methods,
    generate_shipping_methods,
    generate_products,
    generate_transactions
)
from db_utils import (
    insert_users,
    insert_payment_methods,
    insert_shipping_methods,
    insert_products,
    insert_transactions
)

def main():
    users = generate_users(50)
    payment_methods = generate_payment_methods()
    shipping_methods = generate_shipping_methods()
    products = generate_products()
    transactions = generate_transactions(
        n=200,
        users=users,
        products=products,
        payment_methods=payment_methods,
        shipping_methods=shipping_methods
    )

    insert_users(users)
    insert_payment_methods(payment_methods)
    insert_shipping_methods(shipping_methods)
    insert_products(products)
    insert_transactions(transactions)

if __name__ == "__main__":
    main()