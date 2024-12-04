from flask import Flask, request

app = Flask(__name__)

@app.route('/echo', methods=['POST'])
def echo():
    data = request.get_json()  
    print(f"Received request data: {data}")  
    return data  

if __name__ == '__main__':
    app.run(debug=True)
