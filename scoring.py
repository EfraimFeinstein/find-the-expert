#!/usr/bin/env python
""" implement scoring for posts """
from pylab import *
from scipy.stats import percentileofscore
import logging

import topic_classification
import util

acceptedBonus = 0.5     # bonus score to give to an accepted answer
sentimentFactor = 0.7   # how much to weigh the value of comment sentiment relative to a real score

def createPrescoringTables(db):
    c = db.cursor()
    # question prescoring
    c.execute("""
    CREATE TABLE IF NOT EXISTS question_prescoring ( 
        id          int, 
        age         int, 
        score       int, 
        favorites   int, 
        views       int 
    ) 
    SELECT 
        id, 
        TIMESTAMPDIFF(DAY, creation_date, '2012-08-05 12:00:00') AS age, 
        score, 
        favorite_count AS favorites, 
        view_count AS views 
    FROM posts 
    WHERE 
        type_id=1 
    """)
    c.fetchall()

    # answer prescoring
    c.execute("""
    CREATE TABLE IF NOT EXISTS answer_prescoring ( 
        id          int, 
        age         int, 
        score       int, 
        favorites   int, 
        views       int, 
        accepted    bool
    ) 
    SELECT 
        a.id AS id, 
        TIMESTAMPDIFF(DAY, a.creation_date, '2012-08-05 12:00:00') AS age, 
        a.score AS score, 
        q.favorite_count AS favorites, 
        q.view_count AS views, 
        (a.id=q.accepted_answer_id) AS accepted 
    FROM 
        posts AS q 
        INNER JOIN 
        posts as a 
        ON q.id=a.parent_id  
    WHERE 
        a.type_id=2;
    """)
    c.fetchall()

    c.execute("""
    CREATE TABLE IF NOT EXISTS comment_prescoring (
      answer_id int,
      comment_score int,
      INDEX (answer_id)
    )
    SELECT 
      c.post_id AS answer_id,
      SUM(cc.classification) AS comment_score
    FROM
      comments AS c INNER JOIN 
      classified_comments AS cc
      ON cc.comment_id=c.id
    GROUP BY c.post_id
    """)
    c.fetchall()
    c.close()

def getAnswerPrescores(db, postIdList):
    """ return a tuple of age, score, favorites, views divided by age and accepted.
    if an answer is accepted and has a score of 0, add 1
    """
    c = db.cursor()
    c.execute("""
    SELECT 
        id, age, score, favorites, views, accepted 
    FROM
        answer_prescoring
    WHERE 
        id IN (%s)
    """ % ",".join([str(postId) for postId in postIdList])
    )
    results = c.fetchall()
    age = double(array([result[1] for result in results]))
    accepted = array([result[5] if result[5] is not None else 0 for result in results])
    score = double(array([result[2] + accepted[n] for (n, result) in enumerate(results)]))
    favorites = double(array([(result[3] if result[3] is not None else 0) for result in results]))
    views = double(array([(result[4] if result[4] is not None else 0) for result in results]))
    c.close()
    return (age, score, favorites, views, accepted)

def scoreCommentSentiment(db, answerIds):
    c = db.cursor()
    answerIdStr = ",".join([str(answerId) for answerId in answerIds])
    n = c.execute("""SELECT answer_id, comment_score FROM comment_prescoring WHERE answer_id IN (%s)""" % (answerIdStr))
    #c.execute("""SELECT cc.classification FROM classified_comments AS cc INNER JOIN comments AS c ON c.id=cc.comment_id WHERE c.post_id=%d""" % answerId)
    sentiment = {int(s[0]) : int(s[1]) for s in c.fetchall()}
    c.close()
    return sentiment

def scoreUsers(db, query, queryResults, topicModel, cutoffPercentile=75, resultCutoff=0.5):
    """ return the value-weighted score of users in a set of posts
    the posts must be a list including .id, .post, .similarity (relevance)
    """
    class PostDetails:
        def __init__(self, questionId=0, answerId=0, title="", questionRelevance=0, answerRelevance=0):
            self.questionId = questionId
            self.answerId = answerId
            self.title = title
            self.questionRelevance = questionRelevance
            self.answerRelevance = answerRelevance

    class UserScore:
        def __init__(self, userId, user, score, meanRelevance, postIds):
            self.userId = userId
            self.user = user
            self.score = score
            self.meanRelevance = meanRelevance
            self.postIds = postIds
            self.nPosts = len(self.postIds)
            print repr(self)
        def __repr__(self):
            return repr((self.user, self.userId, self.score, self.meanRelevance))
        def starScore(self, cutoffPercentile=75, nStars=5):
            """ convert the score to a number of stars, based on percentileRank (which must be added separately)"""
            self.stars = int(min([nStars, (1+(self.percentileRank - cutoffPercentile - 1.0)//((100.0-cutoffPercentile)/nStars))]))
            return self
    ids = []
    relevance = []
    userIds = []
    postIds = []
    commentSentiment = []
    querySim = topic_classification.TopicModeling.QuerySimilarity(topicModel, query) 
    id2qr = { queryResult.post.id : queryResult for queryResult in queryResults }
    for answer in util.iterateAnswers(db, id2qr):
	useUserId = answer.owner_user_id if answer.owner_user_id is not None else answer.last_editor_user_id
        if useUserId:
	    questionQr = id2qr[answer.parent_id]
            ids.append(answer.id)
            userIds.append(useUserId)
            answerRelevance = 1.0 #querySim.similarity(answer.body)
            relevance.append(questionQr.similarity*answerRelevance)
            postIds.append(
                PostDetails(
                    questionId=answer.parent_id, 
                    answerId=answer.id, 
                    title=questionQr.post.title,
                    questionRelevance=questionQr.similarity, 
                    answerRelevance=answerRelevance
                )
            )
    logging.debug("iterating answers complete, getting prescores")
    ages, scores, favorites, views, accepted = getAnswerPrescores(db, ids)
    logging.debug("got prescores...getting sentiment")
    commentSentimentDict = scoreCommentSentiment(db, ids)
    #commentSentiment = getCommentSentiments(db, ids)
    commentSentiment = array([commentSentimentDict.get(ident, 0) for ident in ids])
    logging.debug("got sentiment...calculating scores...")
    scores += commentSentiment * sentimentFactor
    
    # calculate the scores of the posts for this query using a scoring heuristic
    postIds = array(postIds)
    relevance = array(relevance)
    accepted = array(accepted)
    pctScores = array([percentileofscore(scores, s, 'strict') for s in scores], double)/100.0
    pctFavorites = array([percentileofscore(favorites, f, 'strict') for f in favorites], double)/100.0
    pctViews = array([percentileofscore(views, v, 'strict') for v in views], double)/100.0
    postScore = relevance * (1.0+pctScores) * (1.0+pctFavorites) * (1.0 + pctViews) * (1.0 + acceptedBonus * accepted)
    userIdSet = frozenset(userIds)
    userIds = array(userIds)
    displayNames = {userId : displayName for (userId, displayName) in zip(userIdSet, util.usersById(db, userIdSet))}
    userScores = [
        UserScore(
            user,
            displayNames[user], 
            postScore[userIds==user].sum(), 
            relevance[userIds==user].mean(),
            postIds[userIds==user]
        ) for user in userIdSet]
    allUserScores = array([userScore.score for userScore in userScores], dtype=double)
    for userScore in userScores:
        userScore.percentileRank = percentileofscore(allUserScores, userScore.score)
    logging.debug("sorting users by score...")
    return sorted(filter(lambda us: us.percentileRank >= cutoffPercentile, userScores), key=lambda u: -u.score)

def main():
    """ create precalculated scoring table """
    db = util.makeDbConnection()
    createPrescoringTables(db)
    db.close()

if __name__ == "__main__":
    main()
