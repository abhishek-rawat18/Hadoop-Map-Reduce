from mrjob.job import MRJob
import re
import os

x=['the', 'edu', 'to', 'a', 'and', 'of', 'is', 'for', 'in', 'i', 'graphics', 'cmu', 'it', 'comp', 'cs']
#top 15 words from the previous exercise
    
words_re=re.compile(r"[\w']+") #regular expression to catch words

class MRInvertedIndex(MRJob):
    
    global x
    
    def mapper(self, _, line):
        # mapper takes key and value...here key is ignored using _
        for word in words_re.findall(line):
                if word.lower() in x: 
                    file=os.environ['map_input_file']
                    yield word.lower(),file

                    '''You can also do :-
                       yield word.lower(),file.split("/")[-1]
                       This will give only the file names as output and not the whole path'''

 
    def reducer(self,key,values):
        yield key,list(set(values))
        
 
if __name__ == '__main__':
    MRInvertedIndex.run()
 
