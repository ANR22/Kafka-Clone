from flask import Flask,request
import json
import os
import requests
import pathlib
import os.path

BROKER_PATH = 'F:\\AMOGH\\PES\\sem 5\\bigData\Kafka-Clone\\broker'
BROKER = 3
BROKER_SERVERS = {
    1:"http://127.0.0.1:80",
    2:"http://127.0.0.1:90",
    3:"http://127.0.0.1:100"
}


# print(SUBSCRIBERS)
app = Flask(__name__)

@app.route("/")
def func():
    return "hello"

@app.route("/register-consumer", methods=["POST"])
def reg_consumer():
    consumer_name = request.form['consumer_name']
    topic_name = request.form['topic_name']
    url = request.form['url']
    with open('../subscribers.json', "r") as sub_file:
        SUBSCRIBERS = json.load(sub_file)
    print(SUBSCRIBERS)
    subscriber = {consumer_name: url} 
    
    SUBSCRIBERS[topic_name] = SUBSCRIBERS.get(topic_name,dict()) 
    SUBSCRIBERS[topic_name][consumer_name] = url
    with open('../subscribers.json', "w") as sub_file:
        json.dump(SUBSCRIBERS,sub_file)
    return "s"
    
def send_replica(topic_name,partition_no,curr_offset,msg):
    with open('../topics_info.json', "r") as topics_file:
        topics_data = json.load(topics_file)
    partition_followers = topics_data[topic_name][partition_no]["followers"]
    repl_data={
        "topic_name":topic_name,
        "partition_no":partition_no,
        "offset":curr_offset,
        "msg":msg
    }
    for follower in partition_followers:
        print("folloers",follower)
        server = BROKER_SERVERS[follower]
        try:
            res = requests.post(f'{server}/create-replica',data=repl_data)
            print(follower,res.text)
        except:
            pass


def send_to_consumers(topic_name,data):
    with open('../subscribers.json', "r") as sub_file:
        SUBSCRIBERS = json.load(sub_file)
    
    try:
        topic_subscribers = SUBSCRIBERS[topic_name]
    except:
        topic_subscribers ={}
    # print(topic_subscribers)
    for subscriber,url in topic_subscribers.items():
        try:
            res = requests.post(url, data=data)
            print(res.text)
        except:
            pass
        # print(subscriber,url)
def start_topic(topic_name):
    repl_fact = 3
    no_of_partitions = 3
    leaders_followers = {}
    for num in range(no_of_partitions):
        broker_no = (num+1)%3
        leaders_followers[str(num+1)] = {}
        if broker_no == 0:
            broker_no = 3
        try:
            os.mkdir(f'{BROKER_PATH}{broker_no}\\{topic_name}\\partition{num+1}')
        except:
            os.mkdir(f'{BROKER_PATH}{broker_no}\\{topic_name}')
            os.mkdir(f'{BROKER_PATH}{broker_no}\\{topic_name}\\partition{num+1}')
            
        with open(f'{BROKER_PATH}{broker_no}\\offsets.json', "r") as offsets_file:
            offsets_data = json.load(offsets_file)
        
        offsets_data[topic_name] = {}
        offsets_data[topic_name][str(num+1)] = 0
        
        
        with open(f'{BROKER_PATH}{broker_no}\\offsets.json', "w") as offsets_file:
            offsets_file.write(json.dumps(offsets_data))
        
        
        leaders_followers[str(num+1)]["leader"] = broker_no
        leaders_followers[str(num+1)]["followers"] = []
        repl_broker_no = broker_no
        for r in range(repl_fact-1):
            repl_broker_no = (repl_broker_no + 1) % 3
            if repl_broker_no == 0:
                repl_broker_no = 3
            try:
                os.mkdir(f'{BROKER_PATH}{repl_broker_no}\\{topic_name}\\partition{num+1}')
            except:
                os.mkdir(f'{BROKER_PATH}{repl_broker_no}\\{topic_name}')
                os.mkdir(f'{BROKER_PATH}{repl_broker_no}\\{topic_name}\\partition{num+1}')
                
            leaders_followers[str(num+1)]["followers"].append(repl_broker_no)
    print(leaders_followers)
    
    with open('../topics_info.json',"r") as topics_file:
        topics_data = json.load(topics_file)
        
    topics_data[topic_name] = leaders_followers
    
    with open('../topics_info.json', "w") as topics_file:
        topics_file.write(json.dumps(topics_data))
        
    return topic_name

@app.route('/publish', methods=['POST'])
def prod():
    topic_name = request.form['topic_name']
    msg = request.form['msg']
    # try:
    #     partition_no = request.form['partition_no']
    # except:
    #     partition_no = len(msg) % 3
    
    partition_no = str(len(msg) % 3)
    if partition_no == '0':
        partition_no = '3'
    print(partition_no)
    data = {
        "msg": msg
    }
    
    with open('offsets.json',"r") as off_file:
        offsets = json.load(off_file)
    
    try:
        curr_offset = offsets[topic_name][partition_no]
    except:
        # return f'Broker {BROKER} is not the leader for partition {partition_no} of topic: {topic_name}'
        with open('../topics_info.json', 'r') as topics_file:
            topics_data = json.load(topics_file)
        leader = topics_data[topic_name][str(partition_no)]["leader"]
        try: 
            res = requests.post(f'{BROKER_SERVERS[leader]}/publish', data={"topic_name":topic_name,"msg":msg})
            return res.text
        except:
            return '1'
    
    data_obj = json.dumps(data)
    with open(f'{topic_name}\\partition{partition_no}\\{curr_offset}.json', 'w') as file:
        file.write(data_obj) 
    
    offsets[topic_name][partition_no] += 1
    offsets = json.dumps(offsets)
    
    with open("offsets.json", "w") as off_file:
        off_file.write(offsets)
        

    send_replica(topic_name,partition_no,curr_offset,msg)
    send_to_consumers(topic_name,data)
    
    return 'Success'
# @app.route("/consume",methods=["GET"])
# def consume():
#     topic_name = request.args.get('topic_name')
#     partition_no = request.args.get('partition_no')
#     offset = int(request.args.get('offset'))
#     data = []
#     for num in range(offset, OFFSET+1):
#         with open(f"{topic_name}\\partition{partition_no}\\{num}.json", "r") as c_file:
#             data.append(json.load(c_file))
#             # print(json.load(c_file))
#     return data
def start_topic(topic_name):
    repl_fact = 3
    no_of_partitions = 3
    leaders_followers = {}
    for num in range(no_of_partitions):
        broker_no = (num+1)%3
        leaders_followers[str(num+1)] = {}
        if broker_no == 0:
            broker_no = 3
        try:
            os.mkdir(f'{BROKER_PATH}{broker_no}\\{topic_name}\\partition{num+1}')
        except:
            os.mkdir(f'{BROKER_PATH}{broker_no}\\{topic_name}')
            os.mkdir(f'{BROKER_PATH}{broker_no}\\{topic_name}\\partition{num+1}')
            
        with open(f'{BROKER_PATH}{broker_no}\\offsets.json', "r") as offsets_file:
            offsets_data = json.load(offsets_file)
        
        offsets_data[topic_name] = {}
        offsets_data[topic_name][str(num+1)] = 0
        
        
        with open(f'{BROKER_PATH}{broker_no}\\offsets.json', "w") as offsets_file:
            offsets_file.write(json.dumps(offsets_data))
        
        
        leaders_followers[str(num+1)]["leader"] = broker_no
        leaders_followers[str(num+1)]["followers"] = []
        repl_broker_no = broker_no
        for r in range(repl_fact-1):
            repl_broker_no = (repl_broker_no + 1) % 3
            if repl_broker_no == 0:
                repl_broker_no = 3
            try:
                os.mkdir(f'{BROKER_PATH}{repl_broker_no}\\{topic_name}\\partition{num+1}')
            except:
                os.mkdir(f'{BROKER_PATH}{repl_broker_no}\\{topic_name}')
                os.mkdir(f'{BROKER_PATH}{repl_broker_no}\\{topic_name}\\partition{num+1}')
                
            leaders_followers[str(num+1)]["followers"].append(repl_broker_no)
    print(leaders_followers)
    
    with open('../topics_info.json',"r") as topics_file:
        topics_data = json.load(topics_file)
        
    topics_data[topic_name] = leaders_followers
    
    with open('../topics_info.json', "w") as topics_file:
        topics_file.write(json.dumps(topics_data))
        
    return topic_name

@app.route('/create-topic', methods=['POST'])
def create_topic():
    topic = request.form
    topic_name = topic['topic_name']
    # try:
    #     repl_fact = int(topic['replication_factor'])
    # except:
    #     repl_fact = 3
    # try:
    #     no_of_partitions = int(topic["no_of_partitions"])
    # except:
    #     no_of_partitions = 3
    start_topic(topic_name)
    return f'{topic_name} created. partitions=3, replication factor=3'

@app.route('/create-replica', methods=["POST"])
def create_replica():
    topic_name = request.form["topic_name"]
    partition_no = request.form["partition_no"]
    offset = request.form["offset"]
    msg = request.form["msg"]
    
    data = {
        "msg": msg
    }
    
    data = json.dumps(data)
    with open(f'./{topic_name}/partition{partition_no}/{offset}.json',"w") as file:
        file.write(data)
        
    return "Replica created successfully"

@app.route('/topics-data')
def topics_data():
    with open('../topics_info.json', 'r') as topics_file:
        topics_data = json.load(topics_file)
    topic_name = request.args.get('topic_name')
    partition_no = request.args.get('partition_no')
    leader = topics_data[topic_name][partition_no]["leader"]
    return BROKER_SERVERS[leader]

@app.route('/become-leader', methods=['POST'])
def become_leader():
    topic_name = request.form['topic_name']
    partition_no = request.form['partition_no']
    with open('offsets.json','r') as offsets_file:
        offsets = json.load(offsets_file)
    offsets[topic_name] = offsets.get(topic_name, {})
    n=0
    for path in pathlib.Path(f"{topic_name}/partition{partition_no}/").iterdir():
        if path.is_file():
            n += 1
            
    offsets[topic_name][partition_no] = n
    with open('offsets.json', "w") as offsets_file:
        offsets_file.wrtie(json.dumps(offsets))
        
    return "Leader set"


@app.route('/get-all',methods=["GET"])
def send_all():
    topic_name = request.args.get('topic_name')
    partition = request.args.get('partition_no')
    
    with open('offsets.json','r') as offsets_file:
        offsets = json.load(offsets_file)
    
    offset = offsets[topic_name][partition]
    data = []
    for i in range(offset):
        with open(f'{topic_name}\\partition{partition}\\{i}.json','r') as msg_file:
            msg = json.load(msg_file)
            data.append(msg)
    
    return data

@app.route("/from-beginning",methods=["GET"])
def consume():
    topic_name = request.args.get('topic_name')
    consumer_name = request.args.get('consumer_name')
    with open('offsets.json','r') as offsets_file:
        offsets = json.load(offsets_file)
    with open('../topics_info.json','r') as topics_file:
        topics_data = json.load(topics_file)
    print('topics recieved')
    data = []
    topic_data = topics_data[topic_name]
    for partition,l_f in topic_data.items():
        leader = l_f["leader"]
        if leader == BROKER:
            offset = offsets[topic_name][partition]
            for i in range(offset):
                with open(f'{topic_name}\\partition{partition}\\{i}.json','r') as msg_file:
                    msg = json.load(msg_file)
                    data.append(msg)
            
        else:
            try:
                res = requests.get(f'{BROKER_SERVERS[leader]}/get-all',params={"topic_name":topic_name,"partition_no":partition})
                data.append(json.loads(res.text)[0])
            except:
                pass
    
    return json.dumps(data)


@app.route('/send-heartbeat',methods=['GET'])
def send_heartbeat():
    return 'alive'

if __name__ == "__main__":
    app.run(port=100, debug=True)