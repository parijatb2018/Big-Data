--Extraction

ratings = LOAD '/__dsets/rat.csv' USING PigStorage(',') AS 
    (userID:int, movieID:int, rating:float, epoch:int);
    
describe ratings;

movData = LOAD '/__dsets/mov.csv' USING PigStorage(',') AS 
(movieID:int, movieTitle:chararray,genre:chararray);

--Transformation

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group as movieID, 
    AVG(ratings.rating) AS avgRating, MIN(ratings.epoch) as releaseDate;

fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;

fiveStarsWithData = JOIN fiveStarMovies by movieID, movData by movieID;

intermediateData = foreach fiveStarsWithData generate fiveStarMovies::movieID as movieId, 
movData::movieTitle as title, movData::genre as gerne, fiveStarMovies::avgRating as avgRating, 
fiveStarMovies::releaseDate as releaseDate;

describe intermediateData;

oldestFiveStarMovies = ORDER intermediateData BY releaseDate ;
newestFiveStarMovies = ORDER intermediateData BY releaseDate desc;

fiveStarsWithDataOLD = LIMIT oldestFiveStarMovies 5;

DUMP fiveStarsWithDataOLD; 

fiveStarsWithDataNEW = LIMIT newestFiveStarMovies 5;

DUMP fiveStarsWithDataNEW; 

--Loading

--store fiveStarsWithDataOLD into '/__dsets/result' using PigStorage(',');
--store fiveStarsWithDataNEW into '/__dsets/result' using PigStorage(',');
