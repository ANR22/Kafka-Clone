# from flask import Flask
import requests

parameters = {
    "topic_name":"topic1"
}

BROKER_SERVERS = {
    1:"http://127.0.0.1:80",
    2:"http://127.0.0.1:90",
    3:"http://127.0.0.1:100"
}
data = {"topic_name": "topic2","msg": "hell"}
no_of_brokers = 3
for i in range(no_of_brokers):
    try:
        # topics_data = requests.get(f"{BROKER_SERVERS[i+1]}/topics-data", params=parameters)
        # leader_address = topics_data.text
        x = requests.post(f'{BROKER_SERVERS[i+1]}/publish',data=data)
        if x.text == '1':
            print("Leader Broker not available try after 15s.")
        else:
            print(x.text)
        break
    except:
        print("Broker",i+1,"not available.")
        continue


# broker = "http://127.0.0.1:80/create-topic"
# topic = {
#     "topic_name": "topic2",
# }

# res = requests.post(broker,data=topic)
# print(res.text)

