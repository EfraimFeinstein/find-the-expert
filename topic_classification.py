#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
This module sets up the classification system (the semantic index)

Call it by 
topic_classification [tags]

The corpus name and number of posts per topic are set in config.py

The corpus_name is your choice. I use something like stackoverflow_xml for the stackoverflow database and xml tag.
tags are the tags to include (Note: the more you include, the more memory you need for the matrix!). If it is not given,
only the top tags are included, but note that the top ten tags are 50% of the posts.


"""
import sys
import os
import copy
import logging
import gensim
import nltk
import re
import pprint
import MySQLdb 
import time
import cPickle as pickle
from BeautifulSoup import BeautifulSoup

from config import Config
import util

# tokenizers....
linkstops = [u"http", u"com", u"org", u"www", u":", u"://", u"/", u"."]
punctuators = [".", ",", "-", "/", "&", "\\", "'", '"', ":", ";", "(", ")", "?", "*", "!", "$", "%", "#", "|", "{", "}", "[", "]", "://", "...", "<", ">"]
punctuatorSet = set(punctuators)
domainStops = ["gt", "lt"]
stopwords = set(nltk.corpus.stopwords.words('english') + punctuators + domainStops)
stemmer = nltk.WordNetLemmatizer().lemmatize

def tokenizeText(postText, useStemmer=False):
    """ tokenize some text """
    text = nltk.wordpunct_tokenize(postText)
    words = [w.lower() if not useStemmer else stemmer(w.lower()) for w in text if w.lower() not in stopwords]
    nText = nltk.Text(words)
    return nText.tokens

def tokenizePost(title, question, answers, tags):
    """ tokenize a post, separating text and code """
    # note: a better way to do this would be separating out code and text completely and indexing them separately
    textTokens = []
    codeTokens = []
    linkTokens = []
    for postPart in [title, question]+answers:
        if postPart:
            postSoup = BeautifulSoup(postPart)
            postCode = util.extractCode(postSoup)
            if postCode:
                nCode = nltk.Text([ctoken.lower() for ctoken in nltk.wordpunct_tokenize(postCode) if ctoken not in punctuatorSet])
                codeTokens += nCode.tokens

            postText = util.extractText(postSoup)
            if postText:
                textTokens += tokenizeText(postText, useStemmer=True)
            
            #postLinks = util.extractLinks(postSoup)
            #linkParts = nltk.wordpunct_tokenize(postLinks)
            #links = [link for link in linkParts if link not in linkstops]
            #nLinks = nltk.Text(links)
            #linkTokens += nLinks.tokens

    tagTokens = re.split("[<>]+", tags)
    return (textTokens + codeTokens + linkTokens + tagTokens[1:-1])

class StackOverflowCorpus(object):
    """ abstract corpus for gensim 
    A corpus is a set of documents containing a numerical word list and a dictionary
    linking the numbers to words.
    This class also keeps track of the mapping between corpus "documents" and post ids.
    """
    def __init__(self, db, dictionary, topic=None, postList=None):
        self.t0 = time.time()
        self.tbegin = time.time()
        self.ctr = 0        
        self.dictionary = dictionary
        self.topic = topic
        self.postList = postList
        self.db = db
        self.corpusToPost = {}

    def unicodifyTokens(self, lst):
        for item in lst:
            try:
                yield unicode(item)
            except:
                pass

    def __iter__(self):
        for question in util.iterateQuestions(self.db, self.topic, self.postList):
            answers = [answer for answer in util.iterateAnswers(self.db, [question.id])]
            tokens = tokenizePost(question.title, question.body, [answer.body for answer in answers], question.tags)
            if Config.debug and self.ctr > 0 and (self.ctr % 5000)==0:
                now = time.time()
                print >>sys.stderr, "Posts imported:", self.ctr, "(in %0.1fs, %0.2fpost/s)" % (
                    (now-self.t0), self.ctr/(now-self.tbegin)
                    )
                    
                self.t0 = now
            self.corpusToPost[self.ctr] = question.id 
            self.ctr += 1    
            yield self.dictionary.doc2bow([utoken for utoken in self.unicodifyTokens(tokens)], allow_update=True)

    def saveCorpusToPost(self, fileName):
        f = file(fileName, "wb")
        pickle.dump(self.corpusToPost, f)
        f.close()

    @staticmethod
    def loadCorpusToPost(fileName):
        f = file(fileName, "rb")
        corpusToPost = pickle.load(f)
        f.close()
        return corpusToPost

def testIterate():
    try:
        db=util.makeDbConnection("stackoverflow")
        dictionary = gensim.corpora.dictionary.Dictionary()
        for doc in StackOverflowCorpus(db, dictionary):
            print "doc=", doc, "dictionary=",dictionary
            raw_input()
    finally:
        db.close()

def frequencyDistributionToSQL(db, topic, freqDist):
    c=db.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS %s_frequency_table (
        n_gram       varchar(100),
        frequency    int,
        INDEX (n_gram)
    ) ENGINE=MyISAM DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci""" % (topic))
    c.fetchall()
    
    if Config.debug:
        print >>sys.stderr, "Importing frequency distribution to SQL"
    
    c.executemany(("""INSERT INTO %s_frequency_table (""" % topic) + """
        n_gram,
        frequency
    ) VALUES (
        %s,
        %s
    )""",
    [(ngram, frequency) for (ngram, frequency) in freqDist.items()]
    )
    c.fetchall()
    
    c.close()

def getWholePost(db, postId):
    """ get a (title, post and all its answers) """
    title = None
    wholePost = None
    for question in util.iterateQuestions(db, postList=[postId]):
        answers = "\n\n".join([answer.body for answer in util.iterateAnswers(db, postId)])
        title = question.title
        wholePost = "\n\n".join([question.title, question.body, answers, question.tags])
    return (title, wholePost)

def makeStackOverflowCorpus(fileName, topic=None, usePostList=False, useTags=[]):
    db=util.makeDbConnection(Config.myDb)
    if usePostList:
        print "Using post list"
        if not useTags:
            useTags = util.topTags(db, 200)
        postList = util.tagPosts(db, useTags)
        print "Need to import", len(postList), "posts"
    else:
        postList = None
    dictionary = gensim.corpora.dictionary.Dictionary()
    soCorpus = StackOverflowCorpus(db, dictionary, topic, postList)
    try:
        gensim.corpora.MmCorpus.serialize(fileName + ".mm", soCorpus)
    finally:
        dictionary.save(fileName + ".dict")
        soCorpus.saveCorpusToPost(fileName + ".c2p")
        db.close()
    return len(postList)

def makeTfIdf(fileName):
    """ make TFIDF from a corpus """
    corpus = gensim.corpora.MmCorpus(fileName + ".mm")
    tfidf = gensim.models.TfidfModel(corpus)
    tfidf.save(fileName + ".tfidf")
    return tfidf 

def makeLSI(fileName, nTopics, fromTfidf=False):
    """ make LSI given a corpus filename """
    corpus = gensim.corpora.MmCorpus(fileName + ".mm")
    if fromTfidf:
        print >>sys.stderr, "Converting corpus to TFIDF representation"
        tfidf = gensim.models.TfidfModel.load(fileName + ".tfidf")
        useCorpus = tfidf[corpus]
    else:
        useCorpus = corpus
    dictionary = gensim.corpora.Dictionary.load(fileName + ".dict")
    lsi = gensim.models.LsiModel(useCorpus, id2word=dictionary, num_topics=nTopics)
    lsi.save(fileName + ".lsi")
    return lsi

def makeLDA(fileName, nTopics, fromTfidf=False):
    """ make LDA given a corpus filename """
    corpus = gensim.corpora.MmCorpus(fileName + ".mm")
    if fromTfidf:
        print >>sys.stderr, "Converting corpus to TFIDF representation"
        tfidf = gensim.models.TfidfModel.load(fileName + ".tfidf")
        useCorpus = tfidf[corpus]
    else:
        useCorpus = corpus
    dictionary = gensim.corpora.Dictionary.load(fileName + ".dict")
    lda = gensim.models.LdaModel(useCorpus, id2word=dictionary, num_topics=nTopics)
    lda.save(fileName + ".lda")
    return lda

def ldaTopics(fileName, query):
    """ given a query document, find lda topics """
    lda = gensim.models.LdaModel.load(fileName + ".lda")
    dictionary = gensim.corpora.Dictionary.load(fileName + ".dict")
    queryBow = dictionary.doc2bow(tokenizeText(query, useStemmer=True))
    topics = lda[queryBow]
    return topics

def makeSimilarityIndex(fileName, fromTfIdf=False):
    corpus = gensim.corpora.MmCorpus(fileName + ".mm")
    if fromTfIdf:
        print >>sys.stderr, "Converting corpus to TFIDF representation"
        tfidf = gensim.models.TfidfModel.load(fileName + ".tfidf")
        useCorpus = tfidf[corpus]
    else:
        useCorpus = corpus
    lsi = gensim.models.LsiModel.load(fileName + ".lsi")
    index = gensim.similarities.Similarity(fileName, lsi[useCorpus], len(lsi.show_topics()))
    index.save(fileName + ".index")
    return index

def displayMatches(db, matches, start=0, maxresults=5):
    for match in matches[start:(start+maxresults)]:
        print >>sys.stderr, "Working on match:", match
        post = util.Post.fromPostId(db, match[0])
        print post.title, " SIMILARITY:", match[1]

class QueryResult:
    def __init__(self, db, match, post=None):
        """ convert a similiarityQuery match or post to a QueryResult structure; if post is given, match should be the similarity """
        if post:
            self.id = post.id
            self.post = post
            self.similarity = match
        else:
            self.id = int(match[0])
            self.post = util.Post.fromPostId(db, self.id)
            self.similarity = match[1]

class TopicModeling(object):
    """ class to keep references to all the parts of the topic model (aka index) in memory"""
    def __init__(self, corpusName):
        self.corpusName = corpusName
        self.index = gensim.similarities.Similarity.load(corpusName + ".index")
        self.lsi = gensim.models.LsiModel.load(corpusName + ".lsi")
        self.corpus = gensim.corpora.MmCorpus(corpusName + ".mm")
        self.dictionary = gensim.corpora.Dictionary.load(corpusName + ".dict")
        self.corpusToPost = StackOverflowCorpus.loadCorpusToPost(corpusName + ".c2p")
        self.tfidf = gensim.models.TfidfModel.load(corpusName + ".tfidf")
        self.corpusTfidf = self.tfidf[self.corpus]
    
    def similarityQuery(self, query, cutoff=0):
        """ perform a similarity query. return matching post ids, similarity score, and corpus id """
        logging.debug("tokenizing query...")
        queryBow = self.dictionary.doc2bow(tokenizeText(query, useStemmer=True))
        logging.debug("converting query to LSI space...")
        queryLsi = self.lsi[queryBow]
        logging.debug("querying the index...")
        results = self.index[queryLsi]
        logging.debug("filtering %d results..." % len(results))
        matchingPosts = [(self.corpusToPost[corpusDoc], similarity, corpusDoc ) for corpusDoc, similarity in enumerate(results) if similarity >= cutoff] 
        logging.debug("sorting %d results..." % len(matchingPosts))
        matchingPosts.sort(key=lambda match: -match[1])
        logging.debug("sorting complete...")
        return matchingPosts
    
    class QuerySimilarity:
        """ allow comparisons of the same query to multiple other documents """
        def __init__(self, topicModel, query):
            self.topicModel = topicModel
            self.queryBow = topicModel.dictionary.doc2bow(tokenizeText(unicode(query), useStemmer=True))
            self.queryLsi = topicModel.lsi[topicModel.tfidf[self.queryBow]]

        def similarity(self, document):
            docBow = self.topicModel.dictionary.doc2bow(tokenizePost("", document, [], ""))
            if docBow:
                docLsi = self.topicModel.lsi[self.topicModel.tfidf[docBow]]
                docIndex = gensim.similarities.MatrixSimilarity([docLsi])
                similarity = docIndex[self.queryLsi]
                return similarity[0]
            else:
                print >>sys.stderr, "WARNING: Document did not bow:", document
                return 0.5
            
    def similarityToDocument(self, document, query):
        docBow = self.dictionary.doc2bow(tokenizePost("", document, [], ""))
        if docBow:
            docLsi = self.lsi[self.tfidf[docBow]]
            docIndex = gensim.similarities.MatrixSimilarity([docLsi])
            queryBow = self.dictionary.doc2bow(tokenizeText(unicode(query), useStemmer=True))
            queryLsi = self.lsi[self.tfidf[queryBow]]
            similarity = docIndex[queryLsi]
            return similarity[0]
        else:
            print >>sys.stderr, "WARNING: Document did not bow:", document
            return 0.5

    def queryResults(self, db, query, cutoff=0.5):
        """ return query results as a list of QueryResult instances
        """
        matchingPosts = self.similarityQuery(query, cutoff);
        # link id->similarity
        postMatches = {match[0] : match[1] for match in matchingPosts}
        # get the actual posts, but remove the closed ones
        posts = util.Post.fromPostIds(db, postMatches, removeClosed=True)
        logging.debug("returning %d open posts" % len(posts))
        return [QueryResult(db, postMatches[post.id], post=post) for post in posts]

def main():
    """ generate a dictionary, corpus and index from the Stack Overflow dump """
    corpusName = Config.corpusName
    print >>sys.stderr, "Generating corpus..."
    if os.path.isfile(corpusName + ".mm"):
        corpus = gensim.corpora.MmCorpus(corpusName + ".mm")
        nPosts = len(corpus)
        print >>sys.stderr, "Corpus exists with %d posts. skipping." % nPosts
    else:
        if len(sys.argv) > 1:
            useTags = sys.argv[1:]
        else:
            useTags = []
        nPosts = makeStackOverflowCorpus(corpusName, None, usePostList=True, useTags=useTags)
    print >>sys.stderr, "Making TFIDF representation..."
    if os.path.isfile(corpusName + ".tfidf"):
        print >>sys.stderr, "exists, skipping."
    else:
        makeTfIdf(corpusName)
    print >>sys.stderr, "Making LSI..."
    if os.path.isfile(corpusName + ".lsi"):
        print >>sys.stderr, "exists, skipping."
    else:
        makeLSI(corpusName, nPosts//Config.postsPerTopic, True)
    print >>sys.stderr, "Making similarity index..."
    if os.path.isfile(corpusName + ".index"):
        print >>sys.stderr, "exists, skipping."
    else:
        makeSimilarityIndex(corpusName, True)
    #print >>sys.stderr, "Making LDA topic model..."
    #makeLDA(corpusName, nPosts//100, True)

if __name__ == "__main__":
    print >>sys.stderr , "Using corpus name: %s" % Config.corpusName
    logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.DEBUG)
    main()
