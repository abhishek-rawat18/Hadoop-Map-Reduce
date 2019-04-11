from mrjob.job import MRJob
import re

words_re=re.compile(r"[\w']+") #regular expression to catch words
#use re.compile(r"[a-zA-Z]+") if you only want alphabetical character words

class MRWordFreqCount(MRJob):
    
    def mapper(self, _,line):
        # mapper takes key and value...here key is ignored using _
        for i in words_re.findall(line):
            yield i.lower(),1 
            #converted each word to lower so that lower and upper case words won't differ
        
    def reducer(self,key,value): #key=word , value=counts
        yield key,sum(value)

if __name__ == '__main__':
    MRWordFreqCount.run()
