# -*- coding: utf-8 -*-

import os
import sys

from lib.mongodbHandler import mongodbHandler as mongo

MONGOSERVER = '127.0.0.1'
MONGOPORT = 27017

def getFiles(arg, dirname, files):
    if '/.' in dirname:
        return True
    for f in files:
        if f.startswith('.'):
            continue
        filepath = os.path.join(dirname, f)
        if os.path.isfile(filepath):
            arg.append(filepath)

def saveToDB(storefile):
    pass 

def getUserDir():
    for d in os.listdir('./'):
        if os.path.isdir(d):
            yield d

def getProjectDir(udir):
    for p in os.listdir(udir):
        path = os.path.join(udir, p)
        if os.path.isdir(path):
            yield path

def findSourceFile(rootdir):
    os.chdir(rootdir)
    #udirs = [ d for d in os.listdir('./') if os.path.isdir(d) ]
    #pdirs = [ os.path.join(u, p) for u in udirs for p in os.listdir(u) if os.path.isdir(os.path.join(u, p)) ]
    #del udirs

    def insertToDB(name, files):
        rec = {'full_name' : name,
               'files_path' : files
              }
        client.insertOneRecord(rec)

    for udir in getUserDir():
        for pro in getProjectDir(udir):
            storefile = []
            os.path.walk(pro, getFiles, storefile)
            if pro and storefile:
                insertToDB(pro, storefile)
                #print pro 
                #print storefile
                #print '\n'

if __name__ == '__main__':
    client = mongo(MONGOSERVER, MONGOPORT)
    client.getDB('sourceCode')
    client.getCollection('sourceFilePaths')

    srcdir = sys.argv[1].strip()
    findSourceFile(srcdir)
