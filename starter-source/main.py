from quixstreams import Application
import os
import time
import random
import json

# Set required environment variables for local testing
# os.environ["Quix__Workspace__Id"] = "demo-aiworkshopv2-test"
# os.environ["Quix__Portal__Api"] = "https://portal-api.demo.quix.io"
# os.environ["Quix__Sdk__Token"] = "sdk-78e622c490434e94b5c4b5f7a2c3d4b7"

def generate_sensor_data(sensor_id):
    """Generate realistic sensor data"""
    sensor_locations = ["factory_floor", "warehouse", "office", "outdoor"]
    current_time = int(time.time() * 1000)  # milliseconds
    
    return {
        "sensor_id": sensor_id,
        "location": random.choice(sensor_locations),
        "temperature": round(random.uniform(18.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 80.0), 2),
        "pressure": round(random.uniform(1000.0, 1025.0), 2),
        "vibration": round(random.uniform(0.0, 5.0), 3),
        "timestamp": current_time
    }

def main():
    """Demo sensor data producer"""
    
    # Create application (will connect to Quix Cloud)
    app = Application(
        consumer_group="sensor_data_producer", 
        auto_create_topics=True
    )
    
    # Create output topic
    topic = app.topic("sensor-data")
    
    # Create producer
    with app.get_producer() as producer:
        message_count = 0
        sensor_count = 5
        
        while message_count < 100:  # Stop after 100 messages for testing
            for sensor_id in range(1, sensor_count + 1):
                sensor_data = generate_sensor_data(f"sensor_{sensor_id:03d}")
                
                # Produce message (serialize to JSON)
                producer.produce(
                    topic=topic.name,
                    key=sensor_data["sensor_id"].encode(),
                    value=json.dumps(sensor_data).encode()
                )
                
                print(f"Produced: {sensor_data['sensor_id']} - Temp: {sensor_data['temperature']}°C, Location: {sensor_data['location']}")
                message_count += 1
                
                if message_count >= 100:
                    break
            
            time.sleep(1)  # Send data every second

if __name__ == "__main__":
    main()