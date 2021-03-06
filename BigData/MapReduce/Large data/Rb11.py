from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np


class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
	if len(line.split(','))==4: 
           (userID, movieID, rating, timestamp) = line.split(',')
           yield rating, 1

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    RatingsBreakdown.run()
