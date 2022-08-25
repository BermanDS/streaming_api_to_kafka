# Example of streaming API (Wikipedia) 

Define .env from .env.example with correct credentials settings

Creating for Logs from service directory

```bash
_node > mkdir /var/app_logs
_node > chmod -R 777 /var/app_logs
```
--------------------------------------------------------------------------------------------------
as source of stream we will use 
https://stream.wikimedia.org/v2/stream/recentchange

### Main Architecture of service:

## (1) Input data from API ->  (2) extraction necessary attributes ->  (3) publishing to Kafka

#### (1) and (2) Using custom class object streamAPI with async mode

main procedure self.start_streaming()

-------------------------------------------------------------------------------------------------------

#### (3) Using custom connector DBkafka for publishing data (json) to kafka

for establishing Kafka you can use https://github.com/BermanDS/kafka_broker.git

#### Building by using docker-compose:

```bash
node > docker-compose -f docker-compose.yml up --build -d
```