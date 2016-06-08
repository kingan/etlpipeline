from mainLogger import mainLogger
import os, sys
import time
import boto
import re
from boto.s3.key import Key
import json
from datetime import datetime
import gzip
import pygeoip

class s3checker():
    #Initiate with AWS access and secret key. Should be defined as environment variables.
    def __init__(self):
        self.s3 = boto.connect_s3()
#        self.s3 = boto.connect_s3($AWS_ACCESS_KEY_ID,$AWS_SECRET_ACCESS_KEY)


    # Function to yield .gz files from yi bucket
    def checkBucket(self):
        kin = self.s3.get_bucket('yi-engineering-recruitment')
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


    # Read logs and return successful operations
    def getSuccessList(self):
        with open('/home/ubuntu/etlpipeline/logs/processLog.log','r') as f:
            return map((lambda x:x.split()[3]), filter((lambda x:'SUCCESS' in x), f.read().split('\n')[:-1]))


    # Get contents from S3, parse locally, and return to S3
    def processFile(self, filename):
        k = Key(self.s3.get_bucket('yi-engineering-recruitment'))

        def getFileContent():
            k.key = filename
            k.get_contents_to_filename('/home/ubuntu/etlpipeline/%s'%filename)

            def openFile():
                try:
                    with gzip.open('/home/ubuntu/etlpipeline/%s'%filename, 'r') as g:
                        return map((lambda x:x.split('\t')), g.read().split('\n'))[:-1]
                except:
                    try:
                        with open('/home/ubuntu/etlpipeline/%s'%filename, 'r') as g:
                            return map((lambda x:x.split('\t')), g.read().split('\n'))[:-1]
                    except:
                        pass

            return openFile()



        def setFileContent(filecontent):
            def processLine(fileLine):
                #
                # Convert datetime to timestamp
                def parseDatetime():
                    try:
                        return {"timestamp":(datetime.strptime(reduce((lambda x,y:x+'T'+y),fileLine[:2]),'%Y-%m-%dT%H:%M:%S') - datetime(1970,1,1)).total_seconds()}
                    except:
                        return {"timestamp":""}
#
                # Parse string
                def parseString():
                    try:
                        return {"user_agent":fileLine[5]}
                    except:
                        return {"user_agent":""}
#
                def parseURL():
                    try:
                        return {"url":fileLine[3]}
                    except:
                        return {"url":""}
#
                def parseUserID():
                    try:
                        return {"user_id":fileLine[2]}
                    except:
                        return {"user_id":""}
#
                # Convert IP address to location
                def parseIPAddress():
                    try:
                        data = rawdata.record_by_name(fileLine[4])
                        return {"location":{"longitude":data['longitude'],"latitude":data['latitude'],"country":data['country_name'],"city":data['city']}}
                    except:
                        return {"location":""}
#
                processedLine = parseDatetime()
                processedLine.update(parseString())
                processedLine.update(parseIPAddress())
                processedLine.update(parseURL())
                processedLine.update(parseUserID())
                return processedLine
#
            with gzip.open('/home/ubuntu/etlpipeline/processed/%s'%(filename.split('/')[-1]),'w' ) as g:
                for i in filecontent:
                    g.write(json.dumps(processLine(i)))
            
            # Return to S3
            k.key = 'processed/aking/%s'%filename
            k.set_contents_from_filename('/home/ubuntu/etlpipeline/processed/%s'%(filename.split('/')[-1]))

            return 0

        # Remove local files
        def fileCleanup():
            os.remove('/home/ubuntu/etlpipeline/%s'%filename)
            os.remove('/home/ubuntu/etlpipeline/processed/%s'%(filename.split('/')[-1]))

        setFileContent(getFileContent())
        fileCleanup()
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
