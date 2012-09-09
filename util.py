'''
Basic utility functions for insight project
Created on Aug 14, 2012

@author: efeins
'''
import sys
import logging
import MySQLdb
import re
from BeautifulSoup import BeautifulSoup
from config import Config
from nltk.tokenize import WordPunctTokenizer

class Post:
    def __init__(self, post):
        """ turn a tuple from SELECT * into a structured post """
        self.id = int(post[0])
        self.type_id = int(post[1])
        try:
            self.parent_id = int(post[2])
        except:
            self.parent_id = None
        try:
            self.accepted_answer_id = int(post[3])
        except:
            self.accepted_answer_id = None
        self.creation_date = post[4]
        try:
            self.score = post[5]
        except:
            self.score = None
        try:
            self.view_count = post[6]
        except:
            self.view_count = 0
        self.body = post[7]
        try:
            self.owner_user_id = int(post[8])
        except:
            self.owner_user_id = None    
        try:
            self.last_editor_user_id = int(post[9])
        except:
            self.last_editor_user_id = None
        try:
            self.last_editor_display_name = post[10]
        except:
            self.last_editor_display_name = None
        self.last_activity_date = post[11]
        self.last_edit_date = post[12]
        self.community_owned_date = post[13]
        self.closed_date = post[14]
        try:
            self.title = post[15]
        except:
            self.title = None
        try:
            self.tags = post[16]
        except:
            self.tags = None
        try:
            self.answer_count = int(post[17])
        except:
            self.answer_count = 0
        try:
            self.comment_count = int(post[18])
        except:
            self.comment_count = 0
        try:
            self.favorite_count = int(post[19])
        except:
            self.favorite_count = 0

    @staticmethod
    def fromPostId(db, postId):
        c=db.cursor()
        n = c.execute("""SELECT * FROM posts WHERE id=%d""" % postId)
        if n>0:
            p = c.fetchall()[0]
        c.close()
        return Post(p) if n > 0 else None

    @staticmethod
    def fromPostIds(db, postIds, removeClosed=True):
        c=db.cursor()
        idString = ",".join([str(postId) for postId in postIds])
        n = c.execute(("""SELECT * FROM posts WHERE id IN (%s) """ % idString) + 
          ("AND closed_date IS NULL" if removeClosed else "") +  
          (""" ORDER BY FIELD(id, %s)""" % idString))
        if n > 0:
            posts = [Post(p) for p in c.fetchall()]
        c.close()
        return posts if n > 0 else None

def makeDbConnection(database=Config.mySQLdb):
    db= MySQLdb.connect(
                         host=Config.mySQLhost,
                         user=Config.mySQLuser,
                         passwd=Config.mySQLpasswd,
                         db=database,
                         charset="utf8"
                         )
    db.autocommit(True)
    return db

def significantKeywords(db, minPosts, nLimit=0):
    """ Given a database connection, find the list of significant
    tags (eg, minimum number of posts using the tag)
    """
    c=db.cursor()
    c.execute(("""
        SELECT tag 
            FROM tags
            GROUP BY tag
            HAVING COUNT(*) > %d
            ORDER BY COUNT(*) DESC
        """ % minPosts) +
        ("LIMIT %d" % nLimit if (nLimit > 0) else "")
    )
    tags = [tag[0] for tag in c.fetchall()]
    c.close()
    return tags

def topTags(db, topN=1000):
    """ return the top N tags """
    c=db.cursor()
    c.execute("""
    SELECT
        tag
    FROM tags
    GROUP BY tag
    ORDER BY COUNT(*) DESC
    LIMIT %d
    """ % topN)
    tops = [tag0[0] for tag0 in c.fetchall()]
    c.close()
    return tops 

def tagPosts(db, tags):
    """ return unique post ids for posts by tag """
    c=db.cursor()
    print >>sys.stderr, "Finding tagged posts from", tags
    idents = []
    for tag in tags:
        c.execute("""SELECT post_id FROM tags WHERE tag='%s'""" % tag)
        idents += [int(ident[0]) for ident in c.fetchall()]
    c.close()
    return list(set(idents))

def extractCode(postSoup):
    """ extract and clean up the code from a soup-ed post string,
    return a set of tokens"""
    codes = BeautifulSoup()
    for tag in postSoup.findAll("code"):
        codes.insert(len(codes), tag)
        tag.hidden = True
        if tag.string:
            tag.string = tag.string + u"\n"

    return codes.renderContents()

def extractText(postSoup):
    """ extract the unigram keywords from a string, excluding code.
    Use this on both the post and the post title.
    return a list of tokens 
    """
    for tag in postSoup.findAll(True):
        if tag.name in ("code"):
            tag.extract()
        else:
            tag.hidden=True

    return postSoup.renderContents()

def extractLinks(postSoup):
    linkSoup = BeautifulSoup()
    for tag in postSoup.findAll("a"):
        if "href" in tag:
            linkSoup.insert(len(linkSoup), tag["href"])
    
    return linkSoup.renderContents()

if __name__ == '__main__':
    pass

def iterateQuestions(db, onTopic=None, postList=None, selectRate=5000):
    c=db.cursor()
    
    nResults = selectRate
    resultCtr = 0
    lastId = -100
    while nResults == selectRate:
        if postList:
            thisPostList = ",".join([str(postid) for postid in postList[resultCtr:(resultCtr + selectRate)]])
            sql = """
            SELECT * 
            FROM posts
            WHERE id IN (%s) 
            """ % thisPostList
        elif onTopic:
            sql = """
            SELECT p.* 
            FROM tags AS t 
                INNER JOIN posts AS p 
                ON t.tag='%s' AND p.id=t.post_id
            WHERE p.id > %d AND t.post_id > %d
            LIMIT %d""" % (onTopic, lastId, lastId, selectRate)
        else:
            sql = """SELECT * FROM posts WHERE type_id=1 AND id > %d LIMIT %d""" % (lastId, selectRate)
        nResults = c.execute(sql)
        for post in c.fetchall():
            lastId = int(post[0])
            yield Post(post)
        resultCtr += nResults
    c.close()

def iterateAnswers(db, postIds):
    """ return the answers for a given post """
    c=db.cursor()
    strPostId = ",".join([str(postId) for postId in postIds])
    #logging.debug("Loading answers...")
    c.execute("""SELECT * FROM posts WHERE type_id=2 AND parent_id IN (%s) ORDER BY FIELD(parent_id, %s)""" % (strPostId, strPostId))
    for answer in c.fetchall():
        yield Post(answer)
    c.close()

class Comment:
    def __init__(self, commentTuple):
        self.id = commentTuple[0]
        self.post_id = commentTuple[1]
        self.score = commentTuple[2]
        self.body = commentTuple[3]
        self.creation_date = commentTuple[4]
        self.user_id = commentTuple[5]

def iterateComments(db, post_id):
    """ return the answers for a given post """
    c=db.cursor()
    c.execute("""SELECT * FROM comments WHERE post_id=%d""" % post_id)
    for comment in c.fetchall():
        yield Comment(answer)
    c.close()

def iterateAllComments(db, commentList=None, selectRate=5000):
    c=db.cursor()
    
    nResults = selectRate
    resultCtr = 0
    lastId = -100
    while nResults == selectRate:
        if commentList:
            thisCommentList = ",".join([str(commentid) for commentid in commentList[resultCtr:(resultCtr + selectRate)]])
            sql = """
            SELECT * 
            FROM comments
            WHERE id IN (%s)
            ORDER BY FIELD(id, %s) 
            """ % (thisCommentList, thisCommentList)
        else:
            sql = """SELECT * FROM comments WHERE id > %d LIMIT %d""" % (lastId, selectRate)
        nResults = c.execute(sql)
        for comment in c.fetchall():
            lastId = int(comment[0])
            yield Comment(comment)
        resultCtr += nResults
    c.close()

def usersById(db, userIds):
    c = db.cursor()
    idOrder = ",".join([str(userId) for userId in userIds])
    c.execute("""
    SELECT display_name 
    FROM users 
    WHERE id IN (%s)
    ORDER BY FIELD (id, %s)""" % (idOrder, idOrder))
    users =  [dn0[0] for dn0 in c.fetchall()]
    c.close()
    return users
