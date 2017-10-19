from flask import Flask, jsonify
from flask import make_response

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():
    return "Hello, World!"


@app.route('/<res_id>', methods=['GET'])
def res(res_id):
    return "Hello, {}!".format(res_id)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({'error': 'Not found'}), 404)


if __name__ == '__main__':
    app.run(debug=True)
