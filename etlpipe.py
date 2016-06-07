from mainLogger import mainLogger
import os, sys
import time
import boto
import re
from boto.s3.key import Key
import json
from datetime import datetime
import gzip

class s3checker():
    #Initiate with AWS access and secret key. Should be defined as environment variables.
    def __init__(self):
        self.s3 = boto.connect_s3()


#    Test function to check personal S3 bucket
#    def checkTestBucket(self):
#        kin = self.s3.get_bucket('kingan-test0')
#        return map((lambda x:x.split('/')[1]), map((lambda x:x.name), kin.list(prefix='data/'))[1:])



    # Function to yield .gz files from yi bucket
    def checkBucket(self):
        kin = self.s3.get_bucket('kingan-test0')
#        kin = self.s3.get_bucket('yi-engineering-recruitment')
        fileList = map((lambda x:x.name), kin.list(prefix='data/2014/'))
        #
        #Create directory structure
        def createDirStructure(filename):
            try:
                os.mkdir('/home/ubuntu/etlpipeline/'+filename)
            except:
                pass
         
        map(createDirStructure, filter((lambda x:'.gz' not in x), fileList))
        #
        return filter((lambda x:'.gz' in x), fileList)



    # Test Fn
    def checkFn(self, s):
        return s.checkBucket()



    def getSuccessList(self):
        with open('/home/ubuntu/etlpipeline/logs/processLog.log','r') as f:
            return map((lambda x:x.split()[3]), filter((lambda x:'SUCCESS' in x), f.read().split('\n')[:-1]))


    def processFile(self, filename):
        k = Key(self.s3.get_bucket('kingan-test0'))
#        k = Key(self.s3.get_bucket('yi-engineering-recruitment'))

        def processLine(self, filecontent):
            # Process logic goes here #
            pass

        def getFileContent():
            k.key = filename
            k.get_contents_to_filename('/home/ubuntu/etlpipeline/%s'%filename)

            def openFile():
                try:
                    with gzip.open('/home/ubuntu/etlpipeline/%s'%filename, 'r') as g:
                        return map((lambda x:x.split('\t')), g.read().split('\n'))
                except:
                    try:
                        with open('/home/ubuntu/etlpipeline/%s'%filename, 'r') as g:
                            return map((lambda x:x.split('\t')), g.read().split('\n'))
                    except:
                        pass

            return openFile()


        def setFileContent(filecontent):
            with gzip.open('/home/ubuntu/etlpipeline/processed/%s'%(filename.split('/')[-1]),'w' ) as g:
                map((lambda x:g.write(reduce((lambda x,y:x+' '+y), x))), filecontent)
            k.key = 'processed/aking/%s'%filename
            k.set_contents_from_filename('/home/ubuntu/etlpipeline/processed/%s'%(filename.split('/')[-1]))

        def fileCleanup():
            os.remove('/home/ubuntu/etlpipeline/%s'%filename)
            os.remove('/home/ubuntu/etlpipeline/processed/%s'%(filename.split('/')[-1]))

        setFileContent(getFileContent())
        fileCleanup()
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
