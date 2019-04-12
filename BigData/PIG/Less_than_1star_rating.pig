--Extraction
ratings = LOAD '/__dsets/rat.csv' USING PigStorage(',') AS 
    (userID:int, movieID:int, rating:float, epoch:int);
    
describe ratings;

movData = LOAD '/__dsets/mov.csv' USING PigStorage(',') AS 
(movieID:int, movieTitle:chararray,genre:chararray);

--Transformation

ratingsByMovie = GROUP ratings BY movieID;

avgRatings = FOREACH ratingsByMovie GENERATE group as movieID, 
    AVG(ratings.rating) AS avgRating, COUNT(ratings.rating) as counts;

oneStarMovies = FILTER avgRatings BY avgRating < 1.0;

oneStarsWithData = JOIN oneStarMovies by movieID, movData by movieID;

intermediateData = foreach oneStarsWithData generate oneStarMovies::movieID as movieId, 
movData::movieTitle as title, movData::genre as gerne, oneStarMovies::avgRating as avgRating, 
oneStarMovies::counts as counts;

intermediateResult = filter intermediateData by counts > 1;

finalResult = order intermediateResult by counts desc;
DUMP finalResult; 

--Loading
--store fiveStarsWithDataOLD into '/__dsets/result/onestar.csv' using PigStorage(',');
--store fiveStarsWithDataNEW into '/__dsets/result' using PigStorage(',');

