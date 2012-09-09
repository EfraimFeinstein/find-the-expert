#!/usr/bin/env python
# Source code for the trained classifier.
# The supervised classification training was done using a separate client/server interface, resulting in this classification
# setup.
import nltk
import random
import re

import util
import topic_classification

stopwords = topic_classification.punctuators + ["<", ">", "the", "i", "a", "to", "it", "is", "you", "of", "that", "this"]

features = {
  "thank" : re.compile("than(x|(k[s]?))(\s*you)?"),
  "a lot" : re.compile("\b((a\s?lot)|(so much))"),
  "thanks but" : re.compile("thank[s]?\s*(you)?.*(but|anyway)"),
  "anyway"  : re.compile("anyway"),
  "agree"   : re.compile("agree"),
  "almost"  : re.compile("almost"),
  "disagree"  : re.compile("disagree|(n['o]t\s+agree)"),
  "sorry"   : re.compile("sorry"),
  "also"    : re.compile("also"),
  "but"   : re.compile("but"),
  "works" : re.compile("(does.*)?work(s|ed)?"),
  "not"   : re.compile("((does|(did\s*))((n't)|(\bnot)))|(n[o']t\bwork)"),
  "+"     : re.compile("\b\+\s*\d+\b"),
  "-"     : re.compile("\b\-\s*\d+\b"),
  "@"     : re.compile("@\S"),
  "!"     : re.compile("!"),
  "?"     : re.compile("\?"),
  "smart" : re.compile("\b(clear|smart|clever|(interest(ed|ing))|right|correct|useful|thorough)"),
  "fixed" : re.compile("\bfixed|(solve(s|d))"),
  "needed" : re.compile("(what i (need|want))|(did the trick)"),
  "nice"  : re.compile("\b(perfect|excellent|wonderful|cool|love|ok|nice|good|great|(help(ed|s|ful)?)|appreciate|happy|neat)"),
  "wrong" : re.compile("wrong|incorrect|incomplete|useless|(no use)|unclear"),
  "better"  : re.compile("better"),
  "clarify" : re.compile("clarify")
}

def commentFeatures(comment):
    """extract features from a comment"""
    lcomment = comment.lower()
    return {featureName : bool(features[featureName].search(lcomment)) for featureName in features}

rFeatures = {
  "a" : re.compile("a\s"),
  "an" : re.compile("an\s"),
  "the" : re.compile("the")
}

def randomFeatures(comment):
    lcomment = comment.lower()
    return {featureName : bool(rFeatures[featureName].search(lcomment)) for featureName in rFeatures}

def commentClasses(db):
    """ return a dictionary between trainingId and class """
    c=db.cursor()
    c.execute("""SELECT id, sentiment FROM comment_training""")
    classDict = {identifier : sentiment for (identifier, sentiment) in c.fetchall()}
    c.close()
    return classDict

def iterateCommentsFromTrainingList(db, trainingList):
    c= db.cursor()
    trainingListStr = ",".join([str(trainingId) for trainingId in trainingList])
    c.execute("""SELECT c.* FROM comments as c INNER JOIN comment_training as tr ON c.id=tr.comment_id WHERE tr.id IN (%s) ORDER BY FIELD(tr.id, %s)""" %
      (trainingListStr, trainingListStr))
    for comment in c.fetchall():
        yield util.Comment(comment)
    c.close()

def mapCommentsToTruth(db, trainingIdList, knownClasses):
    return [
        (commentFeatures(comment.body), knownClasses[trainingId]) 
        for (trainingId, comment) in
          zip(trainingIdList, iterateCommentsFromTrainingList(db, trainingIdList))]

def mapCommentTextToTruth(db, trainingIdList, knownClasses):
    return [
        (comment.body, knownClasses[trainingId]) 
        for (trainingId, comment) in
          zip(trainingIdList, iterateCommentsFromTrainingList(db, trainingIdList))]

def getTrainingAndTestingSets(db):
    c = db.cursor()
    # divide into a training set and a testing set
    knownClasses = commentClasses(db)
    trainingIds = knownClasses.keys()
    random.shuffle(trainingIds)
    division = len(trainingIds)/2
    trainingSetIds, testingSetIds = trainingIds[:division], trainingIds[division:]
    trainingSet = mapCommentTextToTruth(db, trainingSetIds, knownClasses)
    testingSet = mapCommentTextToTruth(db, testingSetIds, knownClasses)
    c.close()
    return (trainingSet, testingSet) 

def frequencyDistributions(db):
    fd = {1: nltk.FreqDist(), -1: nltk.FreqDist(), 0: nltk.FreqDist()}
    (trainingSet, testingSet) = getTrainingAndTestingSets(db)
    for (comment, commentClass) in trainingSet:
        tokens = nltk.wordpunct_tokenize(comment)
        fd[commentClass].update([unicode(token.lower()) for token in tokens if token.lower() not in stopwords])
    return fd

def trainedCommentClassifier(db):
    """ return a trained comment classifier """
    c = db.cursor()
    # divide into a training set and a testing set
    knownClasses = commentClasses(db)
    trainingIds = knownClasses.keys() #[kc for (kc, v) in knownClasses.items() if v in [-1, 1]]
    random.shuffle(trainingIds)
    division = int(len(trainingIds)*2.0/3.0)
    trainingSetIds, testingSetIds = trainingIds[:division], trainingIds[division:]
    trainingSet = mapCommentsToTruth(db, trainingSetIds, knownClasses)
    testingSet = mapCommentsToTruth(db, testingSetIds, knownClasses)
    
    classifier = nltk.NaiveBayesClassifier.train(trainingSet)
    c.close()
    print classifier.show_most_informative_features(10)
    print nltk.classify.accuracy(classifier, testingSet)
    return (classifier, trainingSet, testingSet)

def classifyComments(db, classifier, commentList=None):
    c=db.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS classified_comments (
        comment_id int,
        classification int,
        INDEX (comment_id)
    ) ENGINE=MyISAM""")
    c.fetchall()
    commentClasses = [(comment.id, classifier.classify(commentFeatures(comment.body)))
          for comment in util.iterateAllComments(db, commentList=commentList)]
    c.executemany("""
    INSERT INTO classified_comments (
      comment_id, classification
    ) VALUES (%s, %s)""",
      commentClasses)
    c.fetchall()
    c.close()

def main():
    f = file("comment.classifier", "rb")
    classifier = pickle.load(f) 
    f.close()
    db = util.makeDbConnection()
    classifyComments(db, classifier)
    db.close()

if __name__ == "__main__":
    main()
