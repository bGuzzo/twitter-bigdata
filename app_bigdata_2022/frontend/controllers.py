import ast

from flask import Flask, jsonify, request
from flask import render_template

app = Flask(__name__)
labels_hashtag = []
values_hashtag = []
labels_word = []
values_word = []
labels_language = []
values_language = []
labels_mention = []
values_mention = []


# pagina hashtag
@app.route("/hashtag")
def hashtag():
    global labels_hashtag, values_hashtag
    labels_hashtag = []
    values_hashtag = []
    return render_template('hashtag.html', values=values_hashtag, labels=labels_hashtag)


@app.route('/refresh-hashtag')
def refresh_hashtag():
    global labels_hashtag, values_hashtag
    return jsonify(sLabel=labels_hashtag, sData=values_hashtag)


@app.route('/update-hashtag', methods=['POST'])
def update_hashtag():
    global labels_hashtag, values_hashtag
    if not request.form or 'data' not in request.form:
        return "error", 400
    labels_hashtag = ast.literal_eval(request.form['label'])
    values_hashtag = ast.literal_eval(request.form['data'])
    return "success", 201


# Pagina utenti mensionati
@app.route('/update-mention', methods=['POST'])
def update_mention():
    global labels_mention, values_mention
    if not request.form or 'data' not in request.form:
        return "error", 400
    labels_mention = ast.literal_eval(request.form['label'])
    values_mention = ast.literal_eval(request.form['data'])
    return "success", 201


@app.route('/refresh-mention')
def refresh_mention():
    global labels_mention, values_mention
    return jsonify(sLabel=labels_mention, sData=values_mention)


@app.route("/mention")
def mention():
    global labels_mention, values_mention
    labels_mention = []
    values_mention = []
    return render_template('mention.html', values=labels_mention, labels=values_mention)


# pagina lingue
@app.route('/update-lang', methods=['POST'])
def update_lang():
    global labels_language, values_language
    if not request.form or 'data' not in request.form:
        return "error", 400
    labels_language = ast.literal_eval(request.form['label'])
    values_language = ast.literal_eval(request.form['data'])
    return "success", 201


@app.route('/refresh-lang')
def refresh_lang():
    global labels_language, values_language
    return jsonify(sLabel=labels_language, sData=values_language)


@app.route("/lang")
def lang():
    global labels_language, values_language
    labels_language = []
    values_language = []
    return render_template('language.html', values=labels_language, labels=values_language)


# pagina word count
@app.route('/update-word', methods=['POST'])
def update_word():
    global labels_word, values_word
    if not request.form or 'data' not in request.form:
        return "error", 400
    labels_word = ast.literal_eval(request.form['label'])
    values_word = ast.literal_eval(request.form['data'])
    return "success", 201


@app.route('/refresh-word')
def refresh_data_word():
    global labels_word, values_word
    return jsonify(sLabel=labels_word, sData=values_word)


@app.route("/word")
def chart_word():
    global labels_word, values_word
    labels_word = []
    values_word = []
    return render_template('word.html', values=labels_word, labels=values_word)


# Pagina di benvenuto
@app.route("/")
def index_page():
    return render_template('index.html')
