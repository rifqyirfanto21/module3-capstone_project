from faker import Faker
import random
import datetime

fake = Faker("en_US")

def generate_users(n):
    """
    Generate n users with dummy localized data
    """
    email_domains = ["gmail.com", "yahoo.com", "outlook.com"]
    users = []
    for i in range(n):
        full_name = fake.name()
        username = full_name.lower().replace(" ", ".")
        domain = random.choice(email_domains)
        email = f"{username}@{domain}"

        users.append({
            "full_name": full_name,
            "email": email,
            "address": fake.address(),
            "phone_number": fake.phone_number(),
            "created_at": fake.date_time_between(start_date="-1y", end_date="now")
        })
    return users

def generate_payment_methods():
    """
    Generate predefined payment methods dummy data
    """
    methods = [
        {"method_name": "Credit Card", "provider": "Visa"},
        {"method_name": "Credit Card", "provider": "MasterCard"},
        {"method_name": "Credit Card", "provider": "American Express"},
        {"method_name": "PayPal", "provider": "PayPal"},
        {"method_name": "Digital Wallet", "provider": "Apple Pay"},
        {"method_name": "Digital Wallet", "provider": "Google Pay"}
    ]
    for m in methods:
        m["created_at"] = fake.date_time_between(start_date="-1y", end_date="now")
    return methods

def generate_shipping_methods():
    """
    Generate predefined shipping methods dummy data
    """
    shippings = [
        {"carrier_name": "USPS", "shipping_type": "Standard"},
        {"carrier_name": "USPS", "shipping_type": "Priority"},
        {"carrier_name": "FedEx", "shipping_type": "Ground"},
        {"carrier_name": "FedEx", "shipping_type": "Express"},
        {"carrier_name": "UPS", "shipping_type": "Ground"},
        {"carrier_name": "UPS", "shipping_type": "Next Day Air"},
    ]
    for s in shippings:
        s["created_at"] = fake.date_time_between(start_date="-1y", end_date="now")
    return shippings

def generate_products():
    """
    Generate predefined products dummy data
    """
    products_data = {
        "Consoles": [
            {"brand": "Sony", "product_name": "PlayStation 5", "price": 499, "cost": 420},
            {"brand": "Microsoft", "product_name": "Xbox Series X", "price": 499, "cost": 430},
            {"brand": "Nintendo", "product_name": "Nintendo Switch OLED", "price": 349, "cost": 280}
        ],
        "PC Components": [
            {"brand": "NVIDIA", "product_name": "RTX 4080", "price": 1199, "cost": 950},
            {"brand": "NVIDIA", "product_name": "RTX 4070 Ti", "price": 799, "cost": 600},
            {"brand": "AMD", "product_name": "Ryzen 9 7950X", "price": 699, "cost": 500},
            {"brand": "AMD", "product_name": "Ryzen 7 7700X", "price": 399, "cost": 280},
            {"brand": "Intel", "product_name": "Core i9-13900K", "price": 589, "cost": 420},
            {"brand": "Intel", "product_name": "Core i7-13700K", "price": 419, "cost": 310},
            {"brand": "Corsair", "product_name": "Vengeance DDR5 32GB RAM", "price": 189, "cost": 110},
            {"brand": "G.Skill", "product_name": "Trident Z5 DDR5 32GB RAM", "price": 199, "cost": 120}
        ],
        "Laptops": [
            {"brand": "Asus", "product_name": "ROG Zephyrus G15", "price": 1799, "cost": 1300},
            {"brand": "MSI", "product_name": "MSI Raider GE78", "price": 2299, "cost": 1700},
            {"brand": "Razer", "product_name": "Razer Blade 15", "price": 1999, "cost": 1500},
            {"brand": "Apple", "product_name": "MacBook Pro 16 M2", "price": 2499, "cost": 1800}
        ],
        "Peripherals": [
            {"brand": "Logitech", "product_name": "Logitech G Pro X Keyboard", "price": 129, "cost": 75},
            {"brand": "Razer", "product_name": "Razer DeathAdder V3 Mouse", "price": 69, "cost": 35},
            {"brand": "SteelSeries", "product_name": "SteelSeries Arctis Nova Pro Headset", "price": 249, "cost": 150},
            {"brand": "HyperX", "product_name": "HyperX Cloud Alpha Wireless Headset", "price": 199, "cost": 120}
        ],
        "Furniture": [
            {"brand": "Secretlab", "product_name": "Secretlab Titan Evo Gaming Chair", "price": 549, "cost": 300},
            {"brand": "DXRacer", "product_name": "DXRacer Formula Gaming Chair", "price": 399, "cost": 220}
        ]
    }

    products = []
    for category, items in products_data.items():
        for item in items:
            products.append({
                "product_name": item["product_name"],
                "brand": item["brand"],
                "category": category,
                "currency": "USD",
                "price": item["price"],
                "cost": item["cost"],
                "created_at": fake.date_time_between(start_date="-1y", end_date="now")
            })
    return products
    
sample = generate_products()
print(sample)