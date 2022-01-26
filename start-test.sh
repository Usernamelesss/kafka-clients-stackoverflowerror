
echo "Starting producer to setup test, it will take a while..."
PROFILE=produce mvn spring-boot:run
echo "All events published on Kafka"

echo "Starting consumer..."
PROFILE=consume mvn spring-boot:run