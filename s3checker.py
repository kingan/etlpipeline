from mainLogger import mainLogger
import sys
import time
import boto
import re

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

    def mainChecker(self, s, m):
        while True:
            try:
                for filename in s.checkBucket():
                    if(filename not in s.getSuccessList()):
                        m.writeSuccessLog(filename)
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
