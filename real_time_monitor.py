"""
Real-Time Fraud Monitor - Phase 4

This script acts as a Kafka Consumer. It continuously listens to the
'fraud_alerts' topic and aggregates real-time statistics, specifically
calculating the number of fraudulent transactions and the total suspicious
amount per minute.
"""

import json
from datetime import datetime
from kafka import KafkaConsumer

def main():
    print("INFO: Initializing Real-Time Fraud Monitor...")
    print("INFO: Connecting to Kafka broker at localhost:9092...")

    try:
        # 1. Initialize the Kafka Consumer
        # Using localhost as this script runs externally to the Docker network
        consumer = KafkaConsumer(
            'fraud_alerts',
            bootstrap_servers=['localhost:9092'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest'
        )

        print("INFO: Connection established. Listening for incoming alerts...")
        print("-" * 65)

        # 2. Initialize metric aggregators for the 1-minute window
        current_minute = datetime.now().strftime('%Y-%m-%d %H:%M')
        fraud_count_per_min = 0
        total_amount_per_min = 0.0

        # 3. Continuous polling loop
        for message in consumer:
            transaction = message.value
            message_time = datetime.now()
            message_minute = message_time.strftime('%Y-%m-%d %H:%M')

            # Check if the time window (1 minute) has shifted
            if message_minute != current_minute:
                # Output the summary for the completed minute
                print(f"\n--- AGGREGATE SUMMARY FOR WINDOW: {current_minute} ---")
                print(f"Total Suspicious Transactions: {fraud_count_per_min}")
                print(f"Total Suspicious Amount (USD): ${total_amount_per_min:.2f}")
                print("-" * 65 + "\n")

                # Reset aggregators for the new minute
                current_minute = message_minute
                fraud_count_per_min = 0
                total_amount_per_min = 0.0

            # Process the incoming transaction
            tx_id = transaction.get('transaction_id', 'UNKNOWN')
            amount_usd = transaction.get('amount_usd', 0.0)
            reason = transaction.get('response_message', 'No reason provided')

            # Update current window metrics
            fraud_count_per_min += 1
            total_amount_per_min += amount_usd

            # Log individual alert
            print(f"ALERT [{message_time.strftime('%H:%M:%S')}]: TxID: {tx_id} | Amount: ${amount_usd:.2f} USD | Reason: {reason}")

    except KeyboardInterrupt:
        print("\nINFO: Monitor terminated by user. Shutting down gracefully.")
    except Exception as e:
        print(f"CRITICAL ERROR: Consumer execution failed. Details: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()

if __name__ == "__main__":
    main()