# -*- coding: utf-8 -*-
from __future__ import (division, print_function, absolute_import, unicode_literals)

import sys
import pymongo

class mongodbHandler():
    def __init__(self, hostname, port):
        self.hostname = hostname
        self.port = port
        self.conn = None
        self.db = None
        self.coll = None
        self._getConn()

    def _getConn(self):
        try:
            self.conn = pymongo.MongoClient(self.hostname, self.port)
        except pymongo.errors.ConnectionFailure, e:
            print ("Could not connect to the DB server : %s" % e)
            sys.exit(1)
        if not self.conn:
            print ("Could not get the connection !")
            sys.exit(1)

    def getDB(self, dbname):
        self.db = self.conn[dbname]

    def getCollection(self, collname):
        self.coll = self.db[collname]

    def insertOneRecord(self, rec):
        try:
            self.coll.insert(rec, continue_on_error=True)
        except pymongo.errors.DuplicateKeyError, e:
            print ("Insert the record %s to mongodb fail with : %s" % (rec['full_name'] , e))
            sys.exit(1)

    def insertMultiRecords(self, recs):
        if not recs:
            return None
        for rec in recs:
            self.insertOneRecord(rec)
