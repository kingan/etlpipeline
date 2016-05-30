import logging
import sys

class mainLogger():
     def __init__(self):
         logging.basicConfig(filename='/home/ubuntu/etlpipeline/logs/processLog.log', format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=10)

     def writeInitialLog(self):
         logging.info('Hello')

     def writeSuccessLog(self, filename):
         logging.warning('SUCCESS %s processed and stored at %s', filename, '/home/ubuntu/etlpipeline/processed')

     def writeErrorLog(self):
         logging.warning('Unexpected Error')

if __name__ == '__main__':
    m = mainLogger()
    #m.writeSuccessLog()
    m.writeInitialLog()
