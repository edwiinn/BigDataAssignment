# Reccomendation System with ALS

*engine.py* : Define the Reccomendation Engine and wrapping all spark related computation <br>
*app.py* : Flask web app that define RESTful API and its functionality <br> 
*server.py* : Initialises a CherryPy web server for *app.py* and create Spark Context for *engine.py*

## REST API :
+ `Top Ratings` : GET /__{user_id}__/ratings/top/__{movies_count}__
+ `Movie Ratings` : GET /__{user_id}__/ratings/__{movies_id}__
+ `All User Ratings` : GET /__{user_id}__/ratings/
+ `Add Ratings` : POST /__{user_id}__/ratings/ *#raw data __{movie_id,rating}__*

### API Images :
+ Top Ratings <br>
![Alt text](../img/04_image_4.jpg)
+ Movie Ratings <br>
![Alt text](../img/04_image_3.jpg)
+ All User Ratings <br>
![Alt text](../img/04_image_1.jpg)
+ Add Ratings <br>
![Alt text](../img/04_image_2.jpg)


