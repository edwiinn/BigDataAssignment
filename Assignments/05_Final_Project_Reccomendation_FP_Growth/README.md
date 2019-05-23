# Reccomendation Product Black Friday
*engine.py* : Define the Reccomendation Engine and wrapping all spark related computation <br>
*app.py* : Flask web app that define RESTful API, its functionality, initialize *Spark Session* and *CherryPy* web Server <br> 

## REST API :
+ `Create User` : POST /models/__{models_id}__/users - body: user_id : \<int\>, products: \<string\>
+ `Get User by Id` : GET /models/__{models_id}__/users/__{user_id}__
+ `Reccomend Product to User` : GET /models/__{models_id}__/users/__{user_id}__

### API Images :
+ Create User <br>
![Alt text](../img/05_image_1.jpg)
+ Get User by Id <br>
![Alt text](../img/05_image_2.jpg)
+ Reccomend Product to User <br>
Each model reccomend different product(s)
    + Model 1
    ![Alt text](../img/05_image_3.jpg)
    + Model 2
    ![Alt text](../img/05_image_4.jpg)
    +Model 3
    ![Alt text](../img/05_image_5.jpg)


