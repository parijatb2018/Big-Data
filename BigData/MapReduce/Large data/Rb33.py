from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
	    MRStep(reducer=self.reducer_sorted_output)
        ]

    def mapper_get_ratings(self, _, line):
	if len(line.split(','))==4:
          (userID, movieID, rating, timestamp) = line.split(',')
          yield movieID,1

    def reducer_count_ratings(self, key, values):
	  yield sum(values),key

    def reducer_sorted_output(self,count,movies):
	 for movie in movies:
	   if count >= 50000:
	     yield movie, count

if __name__ == '__main__':
    RatingsBreakdown.run()
