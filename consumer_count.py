from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

store_counts = Counter()
total_amount = defaultdict(float)
msg_count = 0

for message in consumer:
    tx = message.value
    store = tx['store']
    amount = tx['amount']

    store_counts[store] += 1
    total_amount[store] += amount
    msg_count += 1

    if msg_count % 10 == 0:
        print(f"\n📊 Podsumowanie po {msg_count} wiadomościach:")
        print(f"{'Sklep':<12} {'Transakcje':>12} {'Suma (PLN)':>12} {'Śr. (PLN)':>12}")
        print("-" * 50)
        for store in sorted(store_counts):
            count = store_counts[store]
            total = total_amount[store]
            avg = total / count
            print(f"{store:<12} {count:>12} {total:>12.2f} {avg:>12.2f}")
        print("-" * 50)
