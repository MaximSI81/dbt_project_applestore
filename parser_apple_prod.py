from datetime import datetime
from selenium import webdriver
from selenium.common import NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent
ua = UserAgent()
import time
import re
import pandas as pd
import numpy as np
from faker import Faker
import boto3, os
from dotenv import load_dotenv
import uuid
load_dotenv()


products_apple = []
service = Service(ChromeDriverManager().install())


# Без прокси

chrome_options = Options()
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
with webdriver.Chrome(service=service, options=chrome_options) as driver:
    driver.set_page_load_timeout(100)  # 30 секунд на загрузку страницы
    driver.implicitly_wait(10)  # 10 секунд на поиск элементов

    driver.get("https://video-shoper.ru/catalog/apple_store.html")
    while True:
        try:
            elem = driver.find_element('xpath', '//div[@class="page-navigation"]/a')
            driver.execute_script("arguments[0].click();", elem)
            time.sleep(3)
        except NoSuchElementException:
            print("❌ Элемент не найден - завершаем пагинацию")
            break
        except Exception as e:
            print(f"⚠️  Другая ошибка: {e}")
            break
    for elem in driver.find_elements('xpath', '//div[@class="item"]'):
        products_apple.append((elem.find_element('xpath', './/div[@class="name"]/a').text,
                                      elem.find_element('xpath', './/div[@class="current current--sale"]').text))
        print(products_apple[-1])

def product_id(string):
    id_str = 1
    for i in string:
        if i:
            id_str += (ord(i) * 100)
    return id_str

def get_release_date(s):
    try:
        return re.findall(r'202\d', s)[0]
    except IndexError:
        return 'no_release'
    
def color(s):
    name_color = ''
    apple_colors = (
        # Основные цвета
        "space gray", "silver", "gold", "rose gold", "graphite", "sierra blue",
        "alpine green", "deep purple", "midnight", "starlight", "blue", "pink",
        "yellow", "green", "purple", "red", "white", "black",

        # Из предоставленных данных
        "синий", "тёмная ночь", "черный", "титановый чёрный", "титановый бежевый",
        "пустынный титан", "голубой", "титан", "титановый синий", "зеленый",
        "ультрамарин", "розовый", "титановый белый", "бирюзовый", "красный",
        "сияющая звезда", "белый", "серый космос", "фиолетовый", "желтый",
        "полуночный черный", "титановый золотой", "сланцевый титан", "оранжевый",
        "серебристый", "темно-зеленый", "румянец", "синий деним", "сливовый",
        "миланская петля", "космический черный", "титановая петля", "золотой",
        "сланцевый", "бежечный", "ultramarine", "desert titanium", "natural titanium",
        "black titanium", "titanium blue", "titanium black", "titanium white",
        "white titanium", "pacific blue", "product(red)", "jet black", "matte black"
    )

    for i in s.split():
        if i.lower() in apple_colors:
            name_color += ' ' + i
    if name_color:
        return name_color.strip()
    return "white"

apple_release_years = {
    "iPhone 13": '2021',
    "iPhone SE": '2022',
    "iPhone 14": '2022',
    "iPhone 15": '2023',
    "iPhone 16": '2024',
    "iPhone 17": '2025'
 }

def storage(s):
    try:
        res = re.findall(r'(\d+)\s*(GB|TB|ГБ|ТБ)', s, re.IGNORECASE)
        for i in res:
            if i[1].lower() in ('tb', 'тб'):
                return i[0] + i[1]
        return ''.join(max(res, key=lambda x: int(x[0])))
    except:
        return 'standart'
    
def get_model(s):
    model = []
    for i in s.split():
        if 'gb' in i.lower() or 'tb' in i.lower():
            break
        else:
            model.append(i)
    model = ' '.join(model)
    return model


fake = Faker('ru_RU')

def generate_apple_data(num_sales):
    # Генерация покупателей
    customers = []
    
    for i in range(1000):
        customers.append({
            'customer_id': f'cust_{uuid.uuid4().hex[:8]}',
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.email(),
            'city': fake.city(),
            'reg_date': fake.date_between('-2y')
        })
    
    products = []

    for product in products_apple:
        release_year = get_release_date(product[0])
        model_ = product[0]
        color_name = color(product[0])
        storage_name = storage(product[0])

        if 'iphone' in product[0].lower() and product[0].split()[0].lower() == 'apple':
            name = 'iPhone'
            model_ = product[0][6:product[0].index('(')]
            model = get_model(model_)
            release_year = apple_release_years[model[:9]]

        elif 'airpods' in product[0].lower() and product[0].split()[0].lower() == 'apple':
            name = 'AirPods'
            model_ = product[0][28:]
            model = model_.replace(storage_name, '').replace(color_name, '')

        elif 'watch' in product[0].lower() and product[0].split()[0].lower() == 'apple':
            name = 'Apple Watch'
            model = ' '.join(model_.split()[:4])

        elif 'ipad' in product[0].lower() and product[0].split()[0].lower() == 'apple':
            name = 'iPad'
            model = model_.replace(storage_name, '').replace(color_name, '')
            model = ' '.join(model_.split()[1:4])

        elif 'macbook' in product[0].lower() and product[0].split()[0].lower() == 'apple':
            name = 'MacBook'
            model = ' '.join(model_.split()[1:4]).replace('"', '')

        elif 'настольный компьютер' in product[0].lower():
            name = 'Mac'
            model = ' '.join(model_.split()[3:5])
        elif 'моноблок' in product[0].lower():
            name = 'Моноблок'
            model = ' '.join(model_.split()[1:4]).replace('"', '')
        elif 'монитор' in product[0].lower():
            name = 'Монитор'
            model = ' '.join(model_.split()[2:5]).replace('"', '')
        else:
            name = 'Accessories'
            storage_name = ''
            model = ' '.join(model_.split()[:4])

        products.append({
                        'product_id': product_id(product[0]),
                        'product_name': f"{model_}",
                        'product_type': name,
                        'model': model,
                        'storage': storage_name,
                        'color': color_name,
                        'release_year': release_year,
                        'current_price_rub': int(''.join(product[1][:-1].split())),
                        'created_date': datetime.now().date()
                        })

    # Генерация продаж
    sales = []
    for i in range(num_sales):
        product = np.random.choice(products)
        customer = np.random.choice(customers)
        
        sales.append({
            'sale_id': f'sale_{uuid.uuid4().hex[:12]}',
            'order_id': f'order_{uuid.uuid4().hex[:16]}',
            'order_date': fake.date_between('-1M'),
            'customer_id': customer['customer_id'],
            'product_id': product['product_id'],
            'quantity': np.random.choice([1, 2, 3], p=[0.7, 0.25, 0.05]),
            'price': product['current_price_rub'],
            'status': np.random.choice(['completed', 'cancelled'], p=[0.97, 0.03]),
            'payment_method': np.random.choice(['card', 'e_wallet', 'cash'], p=[0.6, 0.3, 0.1])
        })
    return (
        pd.DataFrame(sales),
        pd.DataFrame(products),
        pd.DataFrame(customers)
    )


# Генерация данных
sales_df, products_df, customers_df = generate_apple_data(50000)


# Сохраняем в parquet
sales_df.to_parquet('s3_storage/apple_sales.parquet', index=False, engine='pyarrow')
products_df.to_parquet('s3_storage/apple_products.parquet', index=False, engine='pyarrow')  
customers_df.to_parquet('s3_storage/apple_customers.parquet', index=False, engine='pyarrow')





'''def upload_to_minio(file, name):
    s3 = boto3.client('s3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id=os.getenv('S3_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('S3_SECRET_ACCESS_KEY'),)

    
    s3.upload_file(
        Filename=file,
        Bucket='prod',
        Key=name
        )
    

upload_to_minio('s3_storage/apple_sales.parquet', 'apple_sales.parquet')
upload_to_minio('s3_storage/apple_products.parquet', 'apple_products.parquet')
upload_to_minio('s3_storage/apple_customers.parquet', 'apple_customers.parquet')'''