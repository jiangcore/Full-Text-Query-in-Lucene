#coding:utf-8

INDEX_DIR = "IndexFiles.index"

MAX_ROWS = 30

import sys, os, lucene
import mysql.connector
from mysql.connector import errorcode

from lucene import *
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import DirectoryReader
from org.apache.lucene.queryparser.classic import QueryParser, MultiFieldQueryParser,QueryParserBase
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.util import Version

from org.apache.lucene.search import BooleanClause



"""
This script is loosely based on the Lucene (java implementation) demo class
org.apache.lucene.demo.SearchFiles.  It will prompt for a search query, then it
will search the Lucene index in the current directory called 'index' for the
search query entered against the 'contents' field.  It will then display the
'path' and 'name' fields for each of the hits it finds in the index.  Note that
search.close() is currently commented out because it causes a stack overflow in
some cases.
"""

connMysql = None
mysqlcur = None


class SearchFilesDB(object):
    def __init__(self, searcher, analyzer):
        self.connMysql = None
        self.mysqlcur = None
        self.searcher = searcher
        self.analyzer =  analyzer
        self.connectMysql()

    def connectMysql(self):
        """ Connect to the MYSQL database server """
        try:

            # connect to the Mysql server
            self.connMysql = mysql.connector.connect(
                host="10.116.12.141",
                port="3306",
                user="umls",
                password="umls",
                database="umls2018"
            )
            # create a cursor
            self.mysqlcur = self.connMysql.cursor()

            # execute a statement
            print (' Connected to Mysql.')
            print('MYSQL database version:')
            self.mysqlcur.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = self.mysqlcur.fetchone()
            print(db_version)

        except self.mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)

    def getUmlsSynonyms(self, cmd):
        # query = 'SELECT STR FROM MRCONSO WHERE CUI in(select CUI from MRCONSO where STR="{}")  limit {}' .format(cmd,str(MAX_ROWS))
        query = 'SELECT distinct STR FROM MRCONSO WHERE STR like "{}%"  limit {}'.format(cmd,MAX_ROWS)
        print 'synonyms query =', query
        self.mysqlcur.execute(query)
        synonyms = self.mysqlcur.fetchall()
        res = []
        for x in synonyms:
            t= x[0]
            res.append(t)
        print 'getUmlsSynonyms=',res
        return res


    def run(self):
        try:
            while True:
                print
                print "Hit enter with no input to quit."
                print "input order: Note text,chartdate,hospital_expire_flag,icd_codes "
                print "input hints: Car crash,2110-01-01 2120-01-01,1,03819"
                queryInput = raw_input("Query:")

                if queryInput == '':
                    return

                cmds = queryInput.split(",")
                command = ''
                print 'cmds = ', cmds
                querys = []
                fields = []
                occurs =[]
                if len(cmds) > 0 and cmds[0]:
                    query_text = self.getUmlsSynonyms(cmds[0])
                    for x in query_text:
                        querys.append(x)
                        fields.append('text')
                        occurs.append(BooleanClause.Occur.SHOULD)
                if len(cmds) > 1 and cmds[1]:
                    dates = cmds [1].split(" ")
                    querys.append('['+dates[0]+' TO ' +dates[1] +']')
                    fields.append('chartdate')
                    occurs.append(BooleanClause.Occur.MUST)

                if len(cmds) > 2 and cmds[2]:
                    querys.append(cmds [2])
                    fields.append('hospital_expire_flag')
                    occurs.append(BooleanClause.Occur.MUST)
                if len(cmds) > 3 and cmds[3]:
                    querys.append(cmds [3])
                    fields.append('icd9_code')
                    occurs.append(BooleanClause.Occur.MUST)

                #note_id,to_char(n.chartdate, \'YYYY-MM-DD\') chartdate, a.hospital_expire_flag,n.category,a.hadm_id,n.text
                print
                print "Searching for: querys={}\n fields={}\n occurs={}".format(  querys, fields, occurs)

                query = MultiFieldQueryParser.parse(querys,fields,occurs, self.analyzer)

                scoreDocs = self.searcher.search(query, 20).scoreDocs
                for scoreDoc in scoreDocs:
                    doc = self.searcher.doc(scoreDoc.doc)
                    print '*******************'
                    # print ' note_id = {}, chartdate ={}, hospital_expire_flag={}, category={}, icd_codes={}, hadm_id={} '.format(doc.get(""), \
                    #       doc.get("chartdate"), doc.get("hospital_expire_flag"), doc.get("category"),doc.get("icd9_code"), doc.get("hadm_id"))
                    # print ' text = \n{} \n'.format( doc.get('text')[:400])
                    # print 'path:', doc.get("path"), 'name:', doc.get("name")
                print '============================='
                print '======= Query Summary  ======'
                print '\nInput:', queryInput
                print '\nAutually query for MultiFieldQueryParser:' , query
                print '\nTop 10 document IDs: ', scoreDocs[:10]
                print "\n%s total matching documents." % len(scoreDocs)
                if len(scoreDocs)>20:
                    print '\nOnly top 20 are displayed .'
                print '============================='

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        finally:
            if self.connMysql is not None:
                self.connMysql.close()
                print('Database connection closed.')

if __name__ == '__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    print 'lucene', lucene.VERSION
    base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    directory = SimpleFSDirectory(Paths.get(os.path.join(base_dir, INDEX_DIR)))
    print 'directory = ', directory
    searcher = IndexSearcher(DirectoryReader.open(directory))
    analyzer = StandardAnalyzer()

    SearchFilesDB(searcher, analyzer).run()
    del searcher
