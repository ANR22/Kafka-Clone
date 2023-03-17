import requests
import time
import json

BROKER_SERVERS = {
    1:"http://127.0.0.1:80",
    2:"http://127.0.0.1:90",
    3:"http://127.0.0.1:100"
}

broker_states = {
    1: 0,
    2: 0,
    3: 0
}

while True:
    for i in range(3):
        broker_no = i+1
        try:
            res = requests.get(f'{BROKER_SERVERS[broker_no]}/send-heartbeat')
            print(broker_no,res.text)
        except:
            broker_states[broker_no] += 1
        
        if broker_states[broker_no] == 3:
            with open('topics_info.json',"r") as topics_file:
                topics_data = json.load(topics_file)
            for topic,partitions in topics_data.items():
                for partition,l_f in partitions.items():
                    if broker_no == l_f["leader"]:
                        new_leader = topics_data[topic][partition]["followers"].pop(0)
                        topics_data[topic][partition]["leader"] = new_leader
                        data = {
                            "topic_name":topic,
                            "partition_no":partition
                        }
                        print(new_leader)
                        res = requests.post(f'{BROKER_SERVERS[new_leader]}/become-leader',data=data)
                        print(res.text)
            # print(topics_data)
            with open('topics_info.json', "w") as topics_file:
                topics_file.write(json.dumps(topics_data))
    time.sleep(3)