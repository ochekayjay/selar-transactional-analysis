import json
import random
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic,AdminClient

#path = ['/home','/home/footbal','/home/tennis','/payments','/settings']
nameOfClient = ['joe','ebuka','tosin','jad','promise']
#timer = ['123,'3090923','10','212341','99000293','123123','3023','103432','212','10293']
dayTime = ['3pm','2am','1:30pm','2:00am','1:32pm','12pm','7:30am','4:45pm']
#previousPage = ['/home','/home/footbal','/home/tennis','/payments','/settings']
noOfClicks = []
browser = ['google','microsoft','brave','opera mini','firefox']
brand=['olaShoes','JosTalentShow','bibiWears','IkotaEngineeringConference','madeCrotchet','ebukaShades','mealsByTosin','IgocheBespokeClothings']
item=['shoes','dress','ticket','shades','food','native-wears']
states = ['Abia','Adamawa','Akwa Ibom','Anambra','Bauchi','Bayelsa','Benue','Borno','Cross River','Delta','Ebonyi','Edo','Ekiti','Enugu','Fct','Gombe','Imo','Jigawa','Kaduna','Kano','Katsina','Kebbi','Kogi','Kwara','Lagos','Nasarawa','Niger','Ogun','Ondo','Osun','Oyo','Plateau','Rivers','Sokoto','Taraba','Yobe','Zamfara']



a = AdminClient({'bootstrap.servers': 'localhost:19092,localhost:19093,localhost:19094'})

new_topics = [NewTopic(topic, num_partitions=3, replication_factor=2) for topic in ["topic4", "topic5"]]
# Note: In a multi-cluster production scenario, it is more typical to use a replication_factor of 3 for durability.

# Call create_topics to asynchronously create topics. A dict
# of <topic,future> is returned.
fs = a.create_topics(new_topics)

# Wait for each operation to finish.
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))



p = Producer({'bootstrap.servers': 'localhost:19092,localhost:19093,localhost:19094'})

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))



# Choose any base date (e.g. today)
base_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

# Random seconds within a day (0 to 86399)
random_seconds = random.randint(0, 86399)


while 'jay':
    # Choose any base date (e.g. today)
    base_date = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)

    # Random seconds within a day (0 to 86399)
    random_seconds = random.randint(0, 86399)

    stingifiedDate = base_date + timedelta(seconds=random_seconds)
    stingifiedDate = stingifiedDate.isoformat()
    obj = {
        'brand' : random.choice(brand),
        'nameOfClient': random.choice(nameOfClient),
        'clientLocation': random.choice(states),
        'time': random.choice(list(range(0, 86399))),
        'paymentMethod': random.choice(['cash','transfer']),
        'dayTime': random.choice(dayTime),
        'item': random.choice(item),
        'count': random.choice(list(range(0,30))),
        'cost': random.choice(list(range(30000,50000,120))),
        'customerFeedBack': random.choice(['good','very good','excellent','bad','terrible'])
    }
    time.sleep(2)
    randTopic = random.choice(["topic4", "topic5"])
    #for data in some_data_source:
    # Trigger any available delivery report callbacks from previous produce() calls
    p.poll(0)

    # Asynchronously produce a message. The delivery report callback will
    # be triggered from the call to poll() above, or flush() below, when the
    # message has been successfully delivered or failed permanently.
    p.produce(randTopic, json.dumps(obj).encode('utf-8'), callback=delivery_report)

    # Wait for any outstanding messages to be delivered and delivery report
    # callbacks to be triggered.
    p.flush()
    print(obj)

