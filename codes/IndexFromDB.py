#!/usr/bin/env python

INDEX_DIR = "IndexFiles.index"
MAX_ROWS = 10000

import sys, os, lucene, threading, time
from datetime import datetime
from configparser import ConfigParser

from java.nio.file import Paths
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field, FieldType
from org.apache.lucene.index import \
    FieldInfo, IndexWriter, IndexWriterConfig, IndexOptions
from org.apache.lucene.store import SimpleFSDirectory

"""
This class is loosely based on the Lucene (java implementation) demo class
org.apache.lucene.demo.IndexFiles.  It will take a directory as an argument
and will index all of the files in that directory and downward recursively.
It will index on the file path, the file name and the file contents.  The
resulting Lucene index will be placed in the current directory and called
'index'.
"""

import psycopg2 
  

class Ticker(object):

    def __init__(self):
        self.tick = True

    def run(self):
        while self.tick:
            sys.stdout.write('.')
            sys.stdout.flush()
            time.sleep(1.0)

class IndexFiles(object):
    """Usage: python IndexFiles <doc_directory>"""

    def __init__(self, storeDir, analyzer):

        if not os.path.exists(storeDir):
            os.mkdir(storeDir)
        self.conn = None

        store = SimpleFSDirectory(Paths.get(storeDir))
        analyzer = LimitTokenCountAnalyzer(analyzer, 1048576)
        config = IndexWriterConfig(analyzer)
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE)
        writer = IndexWriter(store, config)

        # self.indexDocs(root, writer)
        self.indexDocsFromDB(writer)
        ticker = Ticker()
        print 'commit index\n',
        threading.Thread(target=ticker.run).start()
        writer.commit()
        writer.close()
        ticker.tick = False
        print 'done\n'
        
    def connectToDB(self):
	    """ Connect to the PostgreSQL database server """
	    self.conn = None
	    try:
	        # read connection parameters
	        #params = config(filename='database.ini', section='postgresql')
	 
	        # connect to the PostgreSQL server
	        print('Connecting to the PostgreSQL database...')
	        self.conn = psycopg2.connect(host="10.116.12.100",port="5454",database="mimic", user="mimic_demo", password="mimic_demo")
	 
	        # create a cursor
	        cur = self.conn.cursor()
        
			 # execute a statement
	        print('PostgreSQL database version:')
	        cur.execute('SELECT version()')
 
	        # display the PostgreSQL database server version
	        db_version = cur.fetchone()
	        print(db_version)
        
	    except (Exception, psycopg2.DatabaseError) as error:
	        print(error)
	    # finally:
	    #     if self.conn is not None:  self.conn.close()
         #    print('Database connection closed.')

            
        
    def indexDocsFromDB(self,  writer):
        self.connectToDB()
        cur = self.conn.cursor()

        if not cur : return False

        note_id = FieldType()
        note_id.setStored(True)
        note_id.setTokenized(False)
        note_id.setIndexOptions(IndexOptions.DOCS_AND_FREQS) #
        '''
        True if this field value should be analyzed by the Analyzer
        AbstractField.setIndexOptions:
        DOCS_ONLY: only documents are indexed: term frequencies and positions are omitted
        DOCS_AND_FREQS: only documents and term frequencies are indexed: positions are omitted
        DOCS_AND_FREQS_AND_POSITIONS: full postings: documents, frequencies, and positions
        '''

        chartdate = FieldType()
        chartdate.setStored(False)
        chartdate.setTokenized(True)
        chartdate.setIndexOptions(IndexOptions.DOCS_AND_FREQS)

        hospital_expire_flag = FieldType()
        hospital_expire_flag.setStored(True)
        hospital_expire_flag.setTokenized(False)
        hospital_expire_flag.setIndexOptions(IndexOptions.DOCS_AND_FREQS)

        category = FieldType()
        category.setStored(True)
        category.setTokenized(False)
        category.setIndexOptions(IndexOptions.DOCS_AND_FREQS)

        hadm_id = FieldType()
        hadm_id.setStored(True)
        hadm_id.setTokenized(False)
        hadm_id.setIndexOptions(IndexOptions.DOCS_AND_FREQS)

        icd9_code = FieldType()
        icd9_code.setStored(True)
        icd9_code.setTokenized(True)
        icd9_code.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        text = FieldType()
        text.setStored(True)
        text.setTokenized(True)
        text.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        try:
            #fetch all the Discharge summary notes
            sql=('select n.row_id note_id,to_char(n.chartdate, \'YYYY-MM-DD\') chartdate, a.hospital_expire_flag,n.category,a.hadm_id,n.text '
            'from admissions a,noteevents n where a.hadm_id=n.hadm_id and n.category=\'Discharge summary\' ')
            sql += ' limit '+ str(MAX_ROWS)

            print 'sql = ', sql
            cur.execute(sql)
            # print (cur.fetchone())

            rows = cur.fetchall()
            print ' fetch all '

            i = 0

            for row in  rows:
                doc = Document()
                # print ' row= ',row
                doc.add(Field("note_id", str(row[0]), note_id))
                doc.add(Field("chartdate", str(row[1]) if row[1] else '', chartdate))
                doc.add(Field("hospital_expire_flag", str(row[2]), hospital_expire_flag))
                doc.add(Field("category", row[3], category))
                doc.add(Field("hadm_id", str(row[4]), hadm_id))
                doc.add(Field("text", row[5], text))

                sql1 = ('select i.icd9_code, i.short_title  from  diagnoses_icd d, d_icd_diagnoses i '
                        ' where d.icd9_code = i.icd9_code and hadm_id = ')
                sql1 += str(row[4])
                # print 'sql1 = ', sql1
                cur.execute(sql1)
                rows_icd9 = cur.fetchall()
                icd9s = ''
                for row_icd9 in rows_icd9:
                    icd9s +=str(row_icd9[0]) +','+ str(row_icd9[1])
                # print 'icd9s = ',icd9s

                doc.add(Field("icd9_code", icd9s, icd9_code))

                i += 1
                if i % 100 == 0: print '.',
                writer.addDocument(doc)
 		

                # close the communication with the PostgreSQL
            self.conn.close()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if self.conn is not None:
                self.conn.close()
                print('\n Database connection closed.')
 

    def indexDocs(self, root, writer):

        t1 = FieldType()
        t1.setStored(True)
        t1.setTokenized(False)
        t1.setIndexOptions(IndexOptions.DOCS_AND_FREQS)

        t2 = FieldType()
        t2.setStored(False)
        t2.setTokenized(True)
        t2.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS)

        for root, dirnames, filenames in os.walk(root):
            for filename in filenames:
                if not filename.endswith('.txt'):
                    continue
                print "adding", filename
                try:
                    path = os.path.join(root, filename)
                    file = open(path)
                    contents = unicode(file.read(), 'iso-8859-1')
                    file.close()
                    doc = Document()
                    doc.add(Field("name", filename, t1))
                    doc.add(Field("path", root, t1))
                    if len(contents) > 0:
                        doc.add(Field("contents", contents, t2))
                    else:
                        print "warning: no content in %s" % filename
                    writer.addDocument(doc)
                except Exception, e:
                    print "Failed in indexDocs:", e

if __name__ == '__main__':
    lucene.initVM(vmargs=['-Djava.awt.headless=true'])
    print 'lucene', lucene.VERSION
    start = datetime.now()
    base_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    IndexFiles(os.path.join(base_dir, INDEX_DIR),StandardAnalyzer())
    end = datetime.now()


