from flask import Flask, request

app = Flask(__name__)

@app.route('/echo', methods=['POST'])
def echo():
    data = request.get_json()  # Get the JSON data from the request
    print(f"Received request data: {data}")  # Print request data to the console
    return data  # Echo the received data back to the client

if __name__ == '__main__':
    app.run(debug=True)
