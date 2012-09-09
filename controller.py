#!/usr/bin/env python
'''
Web controller for stackoverflow expert recommender 

@author: efeins
'''
import sys
import os
import MySQLdb
import flask
import urllib
import random
import json
import topic_classification
import util
import scoring
import logging
from config import Config

corpus = Config.corpusName
resultCutoff = 0.5         # use most of the posts unless it's deemed very irrelevant
percentileCutoff=75
database = Config.mySQLdb
topicModel = topic_classification.TopicModeling(corpus)

app = flask.Flask(__name__)

@app.route("/", methods=["GET"])
def root():
    query = flask.request.args.get("q", "")
    logging.debug("query=%s" % query)
    postResults  = []
    userResults = []
    if query:
        logging.debug("connecting to the database...")
        db = util.makeDbConnection(database)
        logging.debug("querying the topic model...")
        postResults = topicModel.queryResults(db, query, resultCutoff)
	logging.debug("%d results returned..." % len(postResults))
        if postResults is not None:
            logging.debug("scoring users..." ) 
            userResults = scoring.scoreUsers(db, query, postResults, topicModel, cutoffPercentile=percentileCutoff, resultCutoff=resultCutoff)
	    logging.debug("star-scoring users...")
            userResults = [userResult.starScore(cutoffPercentile=percentileCutoff, nStars=5) for userResult in userResults]
        db.close()
    return flask.render_template("experts.html", query=query, users=userResults, posts=postResults)

@app.route("/about", methods=["GET"])
def about():
    return flask.render_template("about.html")

@app.route("/me", methods=["GET"])
def me():
    return flask.render_template("me.html")

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    app.run(host=Config.host, debug=Config.debug, port=Config.port)
