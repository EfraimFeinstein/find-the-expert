#!/usr/bin/env python
'''
Convert Stack Overflow dumps to MySQL

Stack Overflow dumps come as massive XML documents. They are processed into the SQL database using Python's SAX
interface 5000 (adjustable) records at a time. Each data type is wrangled into a different table using a subclass of 
BufferedContentHandler (which acts like an abstract class).

Created on Aug 8, 2012

@author: efeins
'''
import sys
import datetime
import re
import MySQLdb 
import xml.sax
import time

from config import Config

class BufferedContentHandler(xml.sax.handler.ContentHandler):
    def __init__(self, cursor, startAt=0, bufferSize=5000):
        xml.sax.handler.ContentHandler.__init__(self)
        self._cursor = cursor
        self._startAt = startAt
        self._ctr = 0
        self._bufferSize = bufferSize
        if Config.debug:
            print >>sys.stderr, "%s: Skipping %d items" % (self.__class__.__name__, startAt)
    
    def appendBuffer(self, attrib):
        """append attribute contents to the buffer (abstract!)"""
        pass
    
    def commitBuffer(self):
        """
        commit the contents of self.buffer to the db
        abstract function: you must implement this in all subclasses! """
        pass
    
    def startDocument(self):
        self.buffer = []
    
    def endDocument(self):
        self.commitBuffer()
        if Config.debug:
            print >> sys.stderr, "%s: Final commit complete." % self.__class__.__name__
    
    def startElement(self, name, attrib):
        if name == "row":
            if Config.debug and self._ctr == self._startAt:
                print >>sys.stderr, "%s: Starting to commit items at %d" % (self.__class__.__name__, self._startAt)
            if self._ctr >= self._startAt:
                self.appendBuffer(attrib)
            self._ctr += 1
        if len(self.buffer)>=self._bufferSize:
            n = len(self.buffer)
            t = time.time()
            self.commitBuffer()
            dt = time.time()-t
            if Config.debug:
                print >>sys.stderr,"%s: %d items committed in %0.1fs" % (self.__class__.__name__, n,dt)
            
    def endElement(self, name):
        pass

    def characters(self, content):
        pass

    def ignorableWhitespace(self, content):
        pass
        
    def processingInstruction(self, target, data):
        pass
    

class PostContentHandler(BufferedContentHandler):
    def startDocument(self):
        BufferedContentHandler.startDocument(self)
        self.tagBuffer = []
    
    def appendBuffer(self, attrib):
        self.buffer.append((
                       attrib["Id"],
                       attrib["PostTypeId"],
                       importIfExists(attrib, "ParentId"),
                       importIfExists(attrib, "AcceptedAnswerId"),
                       importTime(attrib["CreationDate"]),
                       importIfExists(attrib, "Score"),
                       importIfExists(attrib, "ViewCount"),
                       importIfExists(attrib, "Body"),
                       importIfExists(attrib, "OwnerUserId"),
                       importIfExists(attrib, "LastEditorUserId"),
                       importIfExists(attrib, "LastEditorDisplayName"),
                       importTime(attrib["LastActivityDate"]),
                       importTime(importIfExists(attrib, "LastEditDate")),
                       importTime(importIfExists(attrib, "CommunityOwnedDate")),
                       importTime(importIfExists(attrib, "ClosedDate")),
                       importIfExists(attrib, "Title"),
                       importIfExists(attrib, "Tags"),
                       importIfExists(attrib, "AnswerCount"),
                       importIfExists(attrib, "CommentCount"),
                       importIfExists(attrib, "FavoriteCount")
                    ))
        if "Tags" in attrib:
            # insert into tags array by question or answer id
            # the first and last will always be blanked
            for tag in re.split("[<>]+", attrib["Tags"])[1:-1]:
                self.tagBuffer.append((
                                       tag,
                                       attrib["Id"]
                                       )
                                      )
                                
    def commitBuffer(self):
        if (len(self.buffer) > 0):
            try:
                self._cursor.executemany("""
                INSERT INTO posts (
                    id,
                    type_id,
                    parent_id,
                    accepted_answer_id,
                    creation_date,
                    score,
                    view_count,
                    body,
                    owner_user_id,
                    last_editor_user_id,
                    last_editor_display_name,
                    last_activity_date,
                    last_edit_date,
                    community_owned_date,
                    closed_date,
                    title,
                    tags,
                    answer_count,
                    comment_count,
                    favorite_count
                ) VALUES (
                    %s,             # id
                    %s,             # type_id
                    %s,             # parent_id
                    %s,             # accepted_answer_id
                    %s,             # creation_date
                    %s,             # score
                    %s,             # view_count
                    %s,             # body
                    %s,             # owner_user_id
                    %s,             # last_editor_user_id
                    %s,             # last_editor_display_name
                    %s,             # last_activity_date
                    %s,             # last_edit_date
                    %s,             # community_owned_date
                    %s,             # closed_date
                    %s,             # title
                    %s,             # tags
                    %s,             # answer_count
                    %s,             # comment_count
                    %s              # favorite_count
                )
                """, 
                self.buffer
                )
                self._cursor.fetchall()
            except MySQLdb.IntegrityError, ex:
                print "Integrity exception while adding posts in batch: Exception: ", ex
            except MySQLdb.Error, ex:
                print "Exception while adding posts in batch: Exception: ", ex
            except Exception, ex:
                print "Python exception while adding posts in batch: Exception: ", ex
            self.buffer = []
        if len(self.tagBuffer) > 0:
            try:
                self._cursor.executemany("""INSERT INTO tags (
                                        tag,
                                        post_id
                                ) VALUES (
                                    %s,
                                    %s
                                )""", self.tagBuffer
                                )
                self._cursor.fetchall()
            except Exception, ex:
                print "Exception while adding a tag in batch: Exception: ", ex
            self.tagBuffer = []
    

class PostHistoryHandler(BufferedContentHandler):
    def commitBuffer(self):
        try:
            self._cursor.executemany("""
                    INSERT INTO post_history(
                        id,
                        post_history_type_id,
                        post_id,
                        revision_guid,
                        creation_date,
                        user_id,
                        user_display_name,
                        comment,
                        text,
                        close_reason_id
                    ) VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    );
                    """, self.buffer
                    )
            self._cursor.fetchall()
        except MySQLdb.Error, ex:
            print "Exception while adding to history in batch: Exception: ", ex
        self.buffer = []
        
    def appendBuffer(self, attrib):
        self.buffer.append((
                          attrib["Id"], 
                          attrib["PostHistoryTypeId"],
                          attrib["PostId"],
                          attrib["RevisionGUID"],
                          importTime(attrib["CreationDate"]),
                          importIfExists(attrib, "UserId"), 
                          importIfExists(attrib, "UserDisplayName"), 
                          importIfExists(attrib, "Comment"),
                          importIfExists(attrib, "Text"),
                          importIfExists(attrib, "CloseReasonId") 
                        )
        )
            
class CommentHandler(BufferedContentHandler):
    def commitBuffer(self):
        try:
            self._cursor.executemany("""
                    INSERT INTO comments(
                        id,
                        post_id,
                        score,
                        text,
                        creation_date,
                        user_id
                    ) VALUES (
                        %s,        # Id
                        %s,        # PostId
                        %s,        # Score
                        %s,        # Text
                        %s,        # CreationDate
                        %s         # UserId
                    );
                    """,
                    self.buffer
            )
            self._cursor.fetchall()
        except MySQLdb.Error, ex:
            print "Exception while adding comment in batch: Exception: ", ex
        self.buffer = []

    def appendBuffer(self, attrib):
        self.buffer.append((
                          attrib["Id"], 
                          attrib["PostId"],
                          importIfExists(attrib, "Score"),
                          attrib["Text"],
                          importTime(attrib["CreationDate"]),
                          importIfExists(attrib, "UserId")
                         ) 
                    )
            
class VotesHandler(BufferedContentHandler):
    def commitBuffer(self):
        try:
            # determine if the post is a question, answer, or comment
            self._cursor.executemany("""
                    INSERT INTO votes(
                        id,
                        post_id,
                        vote_type,
                        creation_date,
                        user_id,
                        bounty_amount
                    ) VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        %s,
                        %s
                    );
                    """, 
                    self.buffer 
            )
            self._cursor.fetchall()
        except MySQLdb.Error, ex:
            print "Exception while adding vote in batch: Exception: ", ex
        self.buffer = []

    def appendBuffer(self, attrib):
        self.buffer.append((
                          attrib["Id"], 
                          attrib["PostId"],
                          attrib["VoteTypeId"],
                          importTime(attrib["CreationDate"]),
                          importIfExists(attrib, "UserId"),
                          importIfExists(attrib, "BountyAmount")
                         ))
        
                
class BadgesHandler(BufferedContentHandler):
    def commitBuffer(self):
        try:
            self._cursor.executemany("""
                    INSERT INTO badges(
                        user_id,
                        name,
                        date
                    ) VALUES (
                        %s,
                        %s,
                        %s
                    );
                    """, self.buffer)
            self._cursor.fetchall()
        except MySQLdb.Error, ex:
            print "Exception while adding badge in batch: Exception: ", ex
        self.buffer = []
        
    def appendBuffer(self, attrib):
        self.buffer.append( (
                          attrib["UserId"], 
                          attrib["Name"],
                          importTime(attrib["Date"])
                         ) 
                    )

class UserContentHandler(BufferedContentHandler):
    def commitBuffer(self):
        try:
            self._cursor.executemany("""
                INSERT INTO users(
                    id,
                    reputation,
                    creation_date,
                    display_name,
                    email_hash,
                    last_access_date,
                    location,
                    website_url,
                    age,
                    about_me,
                    views,
                    up_votes,
                    down_votes
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s
                );
                """, 
                self.buffer
            )
            self._cursor.fetchall()
        except MySQLdb.Error, ex:
            print "Exception while adding user in batch: Exception: ", ex
        self.buffer = []
    
    def appendBuffer(self, attrib):
        self.buffer.append((
                       attrib["Id"], 
                       attrib["Reputation"],
                       importTime(attrib["CreationDate"]),
                       attrib["DisplayName"],
                       importIfExists(attrib, "EmailHash"),
                       importTime(attrib["LastAccessDate"]), 
                       importIfExists(attrib, "Location"),
                       importIfExists(attrib, "WebsiteUrl"),
                       importIfExists(attrib, "Age"),
                       importIfExists(attrib, "AboutMe"), 
                       attrib["Views"],
                       attrib["UpVotes"],
                       attrib["DownVotes"] 
                    )
                )
                

    
def createDatabase(db, database, engine="MyISAM"):
    """ create a database and tables for the stackoverflow data """
    c = db.cursor()
    try:
        c.execute("""CREATE DATABASE IF NOT EXISTS %s;""" % database)
        c.fetchall()
    except:
        print "Database already exists. Not created."
    
    # create the tables
    # I'm separating out tables for tags, questions, and answers
    #    even though they're different 
    # for the comments table, the post_id is the post it responds
    if engine == "MyISAM":
        userKeys = """
        INDEX (id)
        """
    else:
        userKeys = """
        PRIMARY KEY (id)
        """
    c.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id               int             NOT NULL,
        reputation       int             NOT NULL DEFAULT 0,
        creation_date    datetime        NOT NULL,
        display_name     varchar(255)    ,
        email_hash       char(35)        ,
        last_access_date datetime        NOT NULL,
        location         varchar(255)    ,
        website_url      varchar(255)    ,
        age              int,
        about_me         text            ,
        views            int            NOT NULL DEFAULT 0,
        up_votes         int            NOT NULL DEFAULT 0,
        down_votes       int            NOT NULL DEFAULT 0,
        %s
    ) ENGINE=%s DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci ;
    """ % (userKeys, engine)
    )
    c.fetchall()
    
    if engine=="MyISAM":
        badgeKeys = """
        INDEX (user_id)
        """
    else:
        badgeKeys = """
        FOREIGN KEY (user_id)         REFERENCES users (id)
        """ 
    c.execute("""
    CREATE TABLE IF NOT EXISTS badges (
        user_id          int            NOT NULL,
        name             varchar(255)    NOT NULL,
        date             datetime       NOT NULL,
        %s 
    ) ENGINE=%s DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
    """ % (badgeKeys, engine)
    )
    c.fetchall()
    
    if engine=="MyISAM":
        postKeys = """
        INDEX (id),
        INDEX (owner_user_id),
        INDEX (last_editor_user_id),
        INDEX (type_id),
        INDEX (parent_id),
        INDEX (accepted_answer_id)
        """
    else:
        postKeys = """
        PRIMARY KEY (id),
        FOREIGN KEY (owner_user_id) REFERENCES users (id),
        FOREIGN KEY (last_editor_user_id) REFERENCES users (id),
        INDEX (type_id),
        INDEX (parent_id),
        INDEX (accepted_answer_id)
        """
    c.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id               int            NOT NULL,
        type_id          int            NOT NULL,
        parent_id        int            ,
        accepted_answer_id    int       ,
        creation_date    datetime       NOT NULL,
        score            int            ,
        view_count       int            ,
        body             text  ,
        owner_user_id    int            ,
        last_editor_user_id    int      ,
        last_editor_display_name varchar(255),
        last_activity_date    datetime    ,
        last_edit_date    datetime    ,
        community_owned_date    datetime  ,
        closed_date      datetime        ,
        title            varchar(255)    ,
        tags            varchar(255)    ,
        answer_count    int            DEFAULT 0,
        comment_count    int            DEFAULT 0,
        favorite_count    int            DEFAULT 0,
        %s
    ) ENGINE=%s DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
    """ % (postKeys, engine))
    c.fetchall()
    
    if engine == "MyISAM":
        commentKeys = """
        INDEX (id),
        INDEX (post_id),
        INDEX (user_id)
        """
    else:
        commentKeys = """
        PRIMARY KEY (id),
        FOREIGN KEY (post_id)       REFERENCES posts (id),
        FOREIGN KEY (user_id)         REFERENCES users (id)
        """
    c.execute("""
    CREATE TABLE IF NOT EXISTS comments (
        id                int            NOT NULL,
        post_id         int            ,
        score             int          ,
        text              text   NOT NULL,
        creation_date     datetime       NOT NULL,
        user_id           int            ,
        %s
    ) ENGINE=%s DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
    """ % (commentKeys, engine))
    c.fetchall()
    
    if engine == "MyISAM":
        tagKeys = """
        INDEX (post_id),
        INDEX (tag)
        """
    else:
        tagKeys = """
        FOREIGN KEY (post_id) REFERENCES posts (id),
        INDEX (tag)
        """
    c.execute("""
    CREATE TABLE IF NOT EXISTS tags (
        tag                varchar(255)    NOT NULL,
        post_id          int             ,
        %s
    ) ENGINE=%s DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
    """ % (tagKeys, engine))
    c.fetchall()
    
    if engine == "MyISAM":
        votesKeys = """
        INDEX (id),
        INDEX (user_id)
        """
    else:
        votesKeys = """
        PRIMARY KEY (id),
        FOREIGN KEY (user_id) REFERENCES users (id)
        """
    c.execute("""
    CREATE TABLE IF NOT EXISTS votes (
        id                int            NOT NULL,
        post_id         int            NULL,
        vote_type         int            ,
        creation_date     datetime       NOT NULL,
        user_id           int            NULL,
        bounty_amount     int            ,
        %s
    ) ENGINE=%s;
    """ % (votesKeys, engine))
    c.fetchall()
    
    if engine == "MyISAM":
        postHistoryKeys = """
        INDEX (id),
        INDEX (user_id),
        INDEX (post_id)
        """ 
    else:
        postHistoryKeys = """
        PRIMARY KEY (id),
        FOREIGN KEY (user_id) REFERENCES users (id),
        FOREIGN KEY (post_id) REFERENCES posts (id)
        """
    c.execute("""
    CREATE TABLE IF NOT EXISTS post_history (
        id                int            NOT NULL,
        post_history_type_id  int        NOT NULL,
        post_id        int             ,
        revision_guid    char(50)        ,
        creation_date    datetime        NOT NULL,
        user_id          int             ,
        user_display_name varchar(255)   ,
        comment          text            ,
        text             text           ,
        close_reason_id  int             ,
        %s
    ) ENGINE=%s DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
    """ %(postHistoryKeys, engine)) 
    c.fetchall()
    c.close()

def destroyDatabase(db):
    c = db.cursor()
    c.execute("""DROP DATABASE %s;""" % db)
    c.fetchall()
    c.close()

def importTime(oldTime):
    """ convert an ISO time into what MySQL expects """
    if oldTime:
        try:
            timeTuple = datetime.datetime.strptime(oldTime, "%Y-%m-%dT%H:%M:%S.%f")
            return datetime.datetime.strftime(timeTuple, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            # it's a date not a time -- try it. if it doesn't work, it will raise an exception
            #  again
            timeTuple = datetime.datetime.strptime(oldTime, "%Y-%m-%d")
            return datetime.datetime.strftime(timeTuple, '%Y-%m-%d')

def importUsersTable(db, directory):
    print "Importing user table..."
    c = db.cursor()
    c.execute("SELECT count(*) FROM users;")
    count = int(c.fetchall()[0][0])
    if count > 0:
        print "%d users already exist. Resuming..." % count
    
    parser = xml.sax.make_parser()
    parser.setContentHandler(UserContentHandler(c, count))
    parser.parse(directory + "/users.xml")
                
    c.close()

def importIfExists(attributes, attribName):
    if (attribName in attributes and len(attributes[attribName]) > 0):
        return attributes[attribName]
    else:
        return None

def importPostsTable(db, directory):
    print "Importing posts table using SAX..."
    c = db.cursor()
    
    #print "Clearing the tables and importing posts..."
    #c.execute("DELETE FROM tags;")
    #c.fetchall()
    #c.execute("DELETE FROM posts;")
    #c.fetchall()
    c.execute("SELECT count(*) FROM posts;")
    count = int(c.fetchall()[0][0])
    if count > 0:
        print "%d posts already exist. Resuming..." % count
    
    parser = xml.sax.make_parser()
    parser.setContentHandler(PostContentHandler(c, count))
    parser.parse(directory + "/posts.xml")
                
    c.close()
    
    
def importCommentsTable(db, directory):
    print "Importing comments table..."
    c = db.cursor()
    
    #print "Clearing the tables and importing comments..."
    #c.execute("DELETE FROM comments;")
    #c.fetchall()
    c.execute("SELECT count(*) FROM comments;")
    count = int(c.fetchall()[0][0])
    if count > 0:
        print "%d comments already exist. Resuming..." % count
    
    parser = xml.sax.make_parser()
    parser.setContentHandler(CommentHandler(c, count))
    parser.parse(directory + "/comments.xml")
                
    c.close()

def importPostsHistoryTable(db, directory):
    print "Importing post history table..."
    c = db.cursor()
    #print "Clearing the table and importing post history..."
    #c.execute("DELETE FROM post_history;")
    #c.fetchall()
    c.execute("SELECT COUNT(*) FROM post_history;")
    count = int(c.fetchall()[0][0])
    if count > 0:
        print "%d post histories already exist. Resuming..." % count
    
    parser = xml.sax.make_parser()
    parser.setContentHandler(PostHistoryHandler(c, count))
    parser.parse(directory + "/posthistory.xml")
    c.close()

def importBadgesTable(db, directory):
    print "Importing badges table..."
    c = db.cursor()
    c.execute("SELECT count(*) FROM badges;")
    count = int(c.fetchall()[0][0])
    if count > 0:
        print "%d badges already exist. Resuming..." % count
    
    parser = xml.sax.make_parser()
    parser.setContentHandler(BadgesHandler(c, count))
    parser.parse(directory + "/badges.xml")
                
    c.close()

def importVotesTable(db, directory):
    print "Importing votes table..."
    c = db.cursor()
    #print "Clearing the tables and importing votes..."
    #c.execute("DELETE FROM votes;")
    #c.fetchall()
    c.execute("SELECT count(*) FROM votes;")
    count = int(c.fetchall()[0][0])
    if count > 0:
        print "%d votes already exist. Resuming..." % count
        
    parser = xml.sax.make_parser()
    parser.setContentHandler(VotesHandler(c, count))
    parser.parse(directory + "/votes.xml")
    c.close()

def tagAnswers(db):
    # answer posts are not tagged, so we need to tag them here
    print "Tagging answers..."
    c = db.cursor()
    c.execute("""
    CREATE TABLE answer_tags (
             tag     varchar(255),
             post_id int, 
             INDEX (tag), 
             INDEX(post_id)     
    ) ENGINE=MyISAM DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci
    SELECT
          t.tag AS tag,
          a.id AS post_id
    FROM
        tags AS t INNER JOIN posts AS q ON t.post_id=q.id
                  INNER JOIN posts AS a ON q.id=a.parent_id
    WHERE
        a.type_id=2;
    """)
    c.fetchall()
    c.close()

def linkQuestionsToAnswers(db):
    """ link questions to all answers in the database -- a shortcut for parent_id """
    c=db.cursor()
    c.execute("""
    CREATE TABLE qtoa (
        question    int,
        answer      int,
        INDEX (question)
    )
    SELECT 
        q.id AS question,
        a.id AS answer
    FROM 
        posts AS q INNER JOIN posts AS a
        ON q.id=a.parent_id
    WHERE 
        q.type_id=1 AND 
        a.type_id=2
    """)
    c.fetchall()
    c.close()

def importData(db, directory):
    """ read the data from the directory into the connected database """
    importUsersTable(db, directory)          
    importPostsTable(db, directory)           
    importCommentsTable(db, directory)      
    # These tables are unused:
    #importBadgesTable(db, directory)       
    #importVotesTable(db, directory)       
    #importPostsHistoryTable(db, directory)
    tagAnswers(db)
    linkQuestionsToAnswers(db)
        
def main(database):
    db = MySQLdb.connect(
                        host=Config.mySQLhost, 
                        user=Config.mySQLuser, 
                        passwd=Config.mySQLpasswd, 
                        charset="utf8", 
                        db=database)
    db.autocommit(True)
    db.query("""SET NAMES 'utf8';""")
    # you can't create the db after connecting/user priveleges problems:
    # let the dba create the db: 
    createDatabase(db, database)
    importData(db, Config.sourceDirectory)
    db.close()
    print "Done."
    
    #destroyDatabase(db)
if __name__ == '__main__':
    if len(sys.argv) > 1:
        whichDb = sys.argv[1]
    else:
        whichDb = Config.mySQLdb
    print "Using database", whichDb
    main(whichDb)
