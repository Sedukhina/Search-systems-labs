from flask import Flask, render_template, request

from DB import CreateMainDB
from Source import Search

app = Flask(__name__)

@app.route('/')
def home():
    return render_template('index.html')

@app.route('/search',  methods=['POST'])
def search():
    query = request.form.get('search_bar')
    result = Search(query)
    return render_template("search_result.html", search_results = result, query = query)

if __name__ == '__main__':
    CreateMainDB()
    app.run(debug=True)