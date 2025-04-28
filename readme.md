Install redis, kafka, rabbitmq:
pip install -r requirements.txt

Install docker-compose: 
sudo apt install docker-compose

Run the redis, kafka, rabbitmq servers:
sudo docker-compose up -d

Run the simulation:
python3 multipubsub3.py config.json