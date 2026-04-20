import csv
import json
import time
import os
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Конфигурация
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9093')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sales-data')
DATA_DIR = '/app/data'
DELAY_BETWEEN_MESSAGES = 0.1  # Задержка между сообщениями в секундах

def create_producer():
    """Создание Kafka producer с повторными попытками подключения"""
    max_retries = 30
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Successfully connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except KafkaError as e:
            logger.warning(f"Attempt {attempt + 1}/{max_retries}: Failed to connect to Kafka: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                raise

def read_csv_and_send(producer, file_path):
    """Чтение CSV файла и отправка данных в Kafka"""
    logger.info(f"Reading file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            message_count = 0
            
            for row in reader:
                # Преобразование строки CSV в JSON
                message = {
                    'id': row.get('id', ''),
                    'customer': {
                        'first_name': row.get('customer_first_name', ''),
                        'last_name': row.get('customer_last_name', ''),
                        'age': int(row.get('customer_age', 0)) if row.get('customer_age') else None,
                        'email': row.get('customer_email', ''),
                        'country': row.get('customer_country', ''),
                        'postal_code': row.get('customer_postal_code', ''),
                        'pet_type': row.get('customer_pet_type', ''),
                        'pet_name': row.get('customer_pet_name', ''),
                        'pet_breed': row.get('customer_pet_breed', '')
                    },
                    'seller': {
                        'first_name': row.get('seller_first_name', ''),
                        'last_name': row.get('seller_last_name', ''),
                        'email': row.get('seller_email', ''),
                        'country': row.get('seller_country', ''),
                        'postal_code': row.get('seller_postal_code', '')
                    },
                    'product': {
                        'name': row.get('product_name', ''),
                        'category': row.get('product_category', ''),
                        'price': float(row.get('product_price', 0)) if row.get('product_price') else None,
                        'quantity': int(row.get('product_quantity', 0)) if row.get('product_quantity') else None,
                        'weight': float(row.get('product_weight', 0)) if row.get('product_weight') else None,
                        'color': row.get('product_color', ''),
                        'size': row.get('product_size', ''),
                        'brand': row.get('product_brand', ''),
                        'material': row.get('product_material', ''),
                        'description': row.get('product_description', ''),
                        'rating': float(row.get('product_rating', 0)) if row.get('product_rating') else None,
                        'reviews': int(row.get('product_reviews', 0)) if row.get('product_reviews') else None,
                        'release_date': row.get('product_release_date', ''),
                        'expiry_date': row.get('product_expiry_date', ''),
                        'pet_category': row.get('pet_category', '')
                    },
                    'sale': {
                        'date': row.get('sale_date', ''),
                        'customer_id': row.get('sale_customer_id', ''),
                        'seller_id': row.get('sale_seller_id', ''),
                        'product_id': row.get('sale_product_id', ''),
                        'quantity': int(row.get('sale_quantity', 0)) if row.get('sale_quantity') else None,
                        'total_price': float(row.get('sale_total_price', 0)) if row.get('sale_total_price') else None
                    },
                    'store': {
                        'name': row.get('store_name', ''),
                        'location': row.get('store_location', ''),
                        'city': row.get('store_city', ''),
                        'state': row.get('store_state', ''),
                        'country': row.get('store_country', ''),
                        'phone': row.get('store_phone', ''),
                        'email': row.get('store_email', '')
                    },
                    'supplier': {
                        'name': row.get('supplier_name', ''),
                        'contact': row.get('supplier_contact', ''),
                        'email': row.get('supplier_email', ''),
                        'phone': row.get('supplier_phone', ''),
                        'address': row.get('supplier_address', ''),
                        'city': row.get('supplier_city', ''),
                        'country': row.get('supplier_country', '')
                    }
                }
                
                # Отправка сообщения в Kafka
                future = producer.send(KAFKA_TOPIC, value=message)
                
                try:
                    record_metadata = future.get(timeout=10)
                    message_count += 1
                    
                    if message_count % 100 == 0:
                        logger.info(f"Sent {message_count} messages from {os.path.basename(file_path)}")
                    
                except KafkaError as e:
                    logger.error(f"Failed to send message: {e}")
                
                # Небольшая задержка для эмуляции потока данных
                time.sleep(DELAY_BETWEEN_MESSAGES)
            
            logger.info(f"Completed sending {message_count} messages from {os.path.basename(file_path)}")
            
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")

def main():
    """Основная функция"""
    logger.info("Starting Kafka Producer...")
    
    # Создание producer
    producer = create_producer()
    
    try:
        # Получение списка CSV файлов
        csv_files = sorted([
            os.path.join(DATA_DIR, f) 
            for f in os.listdir(DATA_DIR) 
            if f.endswith('.csv')
        ])
        
        if not csv_files:
            logger.error(f"No CSV files found in {DATA_DIR}")
            return
        
        logger.info(f"Found {len(csv_files)} CSV files")
        
        # Обработка каждого файла
        for csv_file in csv_files:
            read_csv_and_send(producer, csv_file)
        
        logger.info("All files processed successfully!")
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed")

if __name__ == "__main__":
    main()
