
data source from Kaggle competition


Enhancement to be do: automate the whole flow using data orchestration tools

------------------------------
wget https://downloads.apache.org/kafka/3.7.1/kafka_2.12-3.7.1.tgz
tar -xvf kafka_2.12-3.7.1.tgz


-----------------------
wget -q -O - https://download.bell-sw.com/pki/GPG-KEY-bellsoft | sudo apt-key add -
echo "deb https://apt.bell-sw.com/ stable main" | sudo tee /etc/apt/sources.list.d/bellsoft.list

sudo apt-get update
sudo apt-get install bellsoft-java8


Start Zoo-keeper:
-------------------------------
bin/zookeeper-server-start.sh config/zookeeper.properties



Start Kafka-server:
----------------------------------------
cd kafka_2.12-3.3.1
bin/kafka-server-start.sh config/server.properties
"sudo nano config/server.properties" - change ADVERTISED_LISTENERS to public ip of the compute engine instance


Start Producer:
--------------------------
bin/kafka-console-producer.sh --topic demo_test --bootstrap-server 3.106.139.47:9092

Start Consumer:
-------------------------
Duplicate the session & enter in a new console --
cd kafka_2.12-3.3.1
bin/kafka-console-consumer.sh --topic demo_test --bootstrap-server 3.106.139.47:9092
