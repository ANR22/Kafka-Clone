from flask import Flask,request
import requests
import sys

app = Flask(__name__)

@app.route("/", methods=["POST"])
def get_data():
    print(request.form['msg'])
    return "Success"


if __name__ == "__main__":
    requests.post("http://127.0.0.1:80/register-consumer", data={"consumer_name":"nags","topic_name":"topic1","url":"http://127.0.0.1:1001"})
    try:
        flag = sys.argv[1]
        if flag == '--from-beginning':
            res = requests.get("http://127.0.0.1:80/from-beginning",params={"conusumer_name":"nags","topic_name":"topic1"})
            print(res.text)
    except:
        pass
    app.run(debug=True, port=1001)
    