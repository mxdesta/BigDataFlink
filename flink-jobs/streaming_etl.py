"""
Apache Flink Streaming ETL Job (упрощенная версия с Table API)
Читает данные из Kafka, трансформирует в модель звезда и записывает в PostgreSQL
"""

import json
import logging
from datetime import datetime
from typing import Dict, Optional

from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class PostgreSQLConnection:
    """Менеджер подключения к PostgreSQL"""
    
    def __init__(self, host='postgres', port=5432, database='flink_db', 
                 user='flink_user', password='flink_password'):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.cursor = None
    
    def connect(self):
        """Установка соединения с БД"""
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            self.cursor = self.conn.cursor()
            logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def close(self):
        """Закрытие соединения"""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()


def parse_date(date_str: str) -> Optional[datetime]:
    """Парсинг даты из строки"""
    if not date_str:
        return None
    
    formats = ['%m/%d/%Y', '%Y-%m-%d', '%d/%m/%Y']
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue
    return None


def process_record(record_json: str, db: PostgreSQLConnection) -> str:
    """Обработка одной записи"""
    try:
        record = json.loads(record_json)
        
        # Получение или создание записей в таблицах измерений
        customer_key = get_or_create_customer(db, record.get('customer', {}))
        seller_key = get_or_create_seller(db, record.get('seller', {}))
        product_key = get_or_create_product(db, record.get('product', {}))
        store_key = get_or_create_store(db, record.get('store', {}))
        supplier_key = get_or_create_supplier(db, record.get('supplier', {}))
        date_key = get_or_create_date(db, record.get('sale', {}).get('date'))
        
        if not all([customer_key, seller_key, product_key, store_key, supplier_key, date_key]):
            logger.warning(f"Skipping record due to missing keys: {record.get('id')}")
            return f"SKIPPED: {record.get('id')}"
        
        # Вставка в таблицу фактов
        sale_data = record.get('sale', {})
        product_data = record.get('product', {})
        
        db.cursor.execute("""
            INSERT INTO fact_sales (
                customer_key, seller_key, product_key, store_key,
                supplier_key, date_key, sale_quantity, sale_total_price,
                product_quantity
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            customer_key, seller_key, product_key, store_key,
            supplier_key, date_key,
            sale_data.get('quantity'),
            sale_data.get('total_price'),
            product_data.get('quantity')
        ))
        
        db.conn.commit()
        return f"PROCESSED: {record.get('id')}"
        
    except Exception as e:
        logger.error(f"Error processing record: {e}")
        if db and db.conn:
            db.conn.rollback()
        return f"ERROR: {str(e)}"


def get_or_create_customer(db: PostgreSQLConnection, customer_data: Dict) -> Optional[int]:
    """Получить или создать запись клиента"""
    try:
        db.cursor.execute("""
            SELECT customer_id FROM dim_customer 
            WHERE customer_email = %s
        """, (customer_data.get('email'),))
        
        result = db.cursor.fetchone()
        if result:
            return result[0]
        
        db.cursor.execute("""
            INSERT INTO dim_customer (
                customer_first_name, customer_last_name, customer_age,
                customer_email, customer_country, customer_postal_code,
                customer_pet_type, customer_pet_name, customer_pet_breed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING customer_id
        """, (
            customer_data.get('first_name'),
            customer_data.get('last_name'),
            customer_data.get('age'),
            customer_data.get('email'),
            customer_data.get('country'),
            customer_data.get('postal_code'),
            customer_data.get('pet_type'),
            customer_data.get('pet_name'),
            customer_data.get('pet_breed')
        ))
        
        db.conn.commit()
        return db.cursor.fetchone()[0]
        
    except Exception as e:
        logger.error(f"Error in get_or_create_customer: {e}")
        db.conn.rollback()
        return None


def get_or_create_seller(db: PostgreSQLConnection, seller_data: Dict) -> Optional[int]:
    """Получить или создать запись продавца"""
    try:
        db.cursor.execute("""
            SELECT seller_id FROM dim_seller 
            WHERE seller_email = %s
        """, (seller_data.get('email'),))
        
        result = db.cursor.fetchone()
        if result:
            return result[0]
        
        db.cursor.execute("""
            INSERT INTO dim_seller (
                seller_first_name, seller_last_name, seller_email,
                seller_country, seller_postal_code
            ) VALUES (%s, %s, %s, %s, %s)
            RETURNING seller_id
        """, (
            seller_data.get('first_name'),
            seller_data.get('last_name'),
            seller_data.get('email'),
            seller_data.get('country'),
            seller_data.get('postal_code')
        ))
        
        db.conn.commit()
        return db.cursor.fetchone()[0]
        
    except Exception as e:
        logger.error(f"Error in get_or_create_seller: {e}")
        db.conn.rollback()
        return None


def get_or_create_product(db: PostgreSQLConnection, product_data: Dict) -> Optional[int]:
    """Получить или создать запись продукта"""
    try:
        db.cursor.execute("""
            SELECT product_id FROM dim_product 
            WHERE product_name = %s AND product_brand = %s
        """, (product_data.get('name'), product_data.get('brand')))
        
        result = db.cursor.fetchone()
        if result:
            return result[0]
        
        release_date = parse_date(product_data.get('release_date'))
        expiry_date = parse_date(product_data.get('expiry_date'))
        
        db.cursor.execute("""
            INSERT INTO dim_product (
                product_name, product_category, product_price, product_weight,
                product_color, product_size, product_brand, product_material,
                product_description, product_rating, product_reviews,
                product_release_date, product_expiry_date, pet_category
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING product_id
        """, (
            product_data.get('name'),
            product_data.get('category'),
            product_data.get('price'),
            product_data.get('weight'),
            product_data.get('color'),
            product_data.get('size'),
            product_data.get('brand'),
            product_data.get('material'),
            product_data.get('description'),
            product_data.get('rating'),
            product_data.get('reviews'),
            release_date,
            expiry_date,
            product_data.get('pet_category')
        ))
        
        db.conn.commit()
        return db.cursor.fetchone()[0]
        
    except Exception as e:
        logger.error(f"Error in get_or_create_product: {e}")
        db.conn.rollback()
        return None


def get_or_create_store(db: PostgreSQLConnection, store_data: Dict) -> Optional[int]:
    """Получить или создать запись магазина"""
    try:
        db.cursor.execute("""
            SELECT store_id FROM dim_store 
            WHERE store_name = %s AND store_city = %s
        """, (store_data.get('name'), store_data.get('city')))
        
        result = db.cursor.fetchone()
        if result:
            return result[0]
        
        db.cursor.execute("""
            INSERT INTO dim_store (
                store_name, store_location, store_city, store_state,
                store_country, store_phone, store_email
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING store_id
        """, (
            store_data.get('name'),
            store_data.get('location'),
            store_data.get('city'),
            store_data.get('state'),
            store_data.get('country'),
            store_data.get('phone'),
            store_data.get('email')
        ))
        
        db.conn.commit()
        return db.cursor.fetchone()[0]
        
    except Exception as e:
        logger.error(f"Error in get_or_create_store: {e}")
        db.conn.rollback()
        return None


def get_or_create_supplier(db: PostgreSQLConnection, supplier_data: Dict) -> Optional[int]:
    """Получить или создать запись поставщика"""
    try:
        db.cursor.execute("""
            SELECT supplier_id FROM dim_supplier 
            WHERE supplier_email = %s
        """, (supplier_data.get('email'),))
        
        result = db.cursor.fetchone()
        if result:
            return result[0]
        
        db.cursor.execute("""
            INSERT INTO dim_supplier (
                supplier_name, supplier_contact, supplier_email,
                supplier_phone, supplier_address, supplier_city, supplier_country
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING supplier_id
        """, (
            supplier_data.get('name'),
            supplier_data.get('contact'),
            supplier_data.get('email'),
            supplier_data.get('phone'),
            supplier_data.get('address'),
            supplier_data.get('city'),
            supplier_data.get('country')
        ))
        
        db.conn.commit()
        return db.cursor.fetchone()[0]
        
    except Exception as e:
        logger.error(f"Error in get_or_create_supplier: {e}")
        db.conn.rollback()
        return None


def get_or_create_date(db: PostgreSQLConnection, date_str: str) -> Optional[int]:
    """Получить или создать запись даты"""
    try:
        date_obj = parse_date(date_str)
        if not date_obj:
            return None
        
        db.cursor.execute("""
            SELECT date_id FROM dim_date WHERE full_date = %s
        """, (date_obj,))
        
        result = db.cursor.fetchone()
        if result:
            return result[0]
        
        day_names = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                      'July', 'August', 'September', 'October', 'November', 'December']
        
        db.cursor.execute("""
            INSERT INTO dim_date (
                full_date, day, month, year, quarter, day_of_week,
                day_name, month_name, is_weekend
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING date_id
        """, (
            date_obj,
            date_obj.day,
            date_obj.month,
            date_obj.year,
            (date_obj.month - 1) // 3 + 1,
            date_obj.weekday(),
            day_names[date_obj.weekday()],
            month_names[date_obj.month - 1],
            date_obj.weekday() >= 5
        ))
        
        db.conn.commit()
        return db.cursor.fetchone()[0]
        
    except Exception as e:
        logger.error(f"Error in get_or_create_date: {e}")
        db.conn.rollback()
        return None


def main():
    """Основная функция - использует простой consumer вместо Flink"""
    logger.info("Starting Kafka to PostgreSQL ETL...")
    
    # Подключение к PostgreSQL
    db = PostgreSQLConnection()
    db.connect()
    
    # Простой Kafka consumer
    from kafka import KafkaConsumer
    import time
    
    # Ожидание готовности Kafka
    max_retries = 30
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                'sales-data',
                bootstrap_servers='kafka:9093',
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='flink-consumer-group',
                value_deserializer=lambda x: x.decode('utf-8')
            )
            logger.info("Successfully connected to Kafka")
            break
        except Exception as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                raise
    
    # Обработка сообщений
    processed_count = 0
    try:
        logger.info("Starting to consume messages from Kafka...")
        for message in consumer:
            result = process_record(message.value, db)
            processed_count += 1
            
            if processed_count % 100 == 0:
                logger.info(f"Processed {processed_count} records")
                
    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()
        db.close()
        logger.info(f"Total processed records: {processed_count}")


if __name__ == "__main__":
    main()
