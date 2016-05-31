from mainLogger import mainLogger
import sys
import time
import boto
import re
from boto.s3.key import Key
import json
from datetime import datetime
import gzip

class s3checker():
    def __init__(self):
        self.s3 = boto.connect_s3()

    def checkBucket(self):
        kin = self.s3.get_bucket('kingan-test0')
        return map((lambda x:x.name), kin.list())

    def checkFn(self, s):
        return s.checkBucket()

    def getSuccessList(self):
        with open('/home/ubuntu/etlpipeline/logs/processLog.log','r') as f:
            return map((lambda x:x.split()[3]), filter((lambda x:'SUCCESS' in x), f.read().split('\n')[:-1]))


    def processFile(self, filename):
        k = Key(self.s3.get_bucket('kingan-test0'))

        def processLine(self, filecontent):
            pass

        def getFileContent():
            k.key = filename
            k.get_contents_to_filename('/home/ubuntu/etlpipeline/data/%s'%filename)
            with gzip.open('/home/ubuntu/etlpipeline/data/%s'%filename, 'r') as g:
                return map((lambda x:x.split('\t')), g.read().split('\n'))

        def setFileContent(filecontent):
            with gzip.open('/home/ubuntu/etlpipeline/data/processed_%s'%filename,'w' ) as g:
                map((lambda x:g.write(reduce((lambda x,y:x+' '+y), x))), filecontent)
            k.key = 'processed_%s'%filename
            k.set_contents_from_filename('/home/ubuntu/etlpipeline/data/processed_%s'%filename)

        setFileContent(getFileContent())
        #map(setFileContent, map(processLine, getFileContent()))
        return 0


    def mainChecker(self, s, m):
        while True:
            try:
                for filename in s.checkBucket():
                    if(filename not in s.getSuccessList()):
                        if(s.processFile(filename)==0):
                            m.writeSuccessLog(filename)
                        else:
                            pass
                    else:
                        pass
                time.sleep(2)
            except:
                m.writeErrorLog()
                sys.exit()


if __name__ == "__main__":
    s = s3checker()
    m = mainLogger()
    print s.mainChecker(s, m)
