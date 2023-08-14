#Alba Rodriguez Berenguel

#CUNEF 
#ETL

#-------------------------

#Librerias
library(dplyr)
library(DBI)
library(stats)

#-------------------------

#Base de datos
bd <- dbConnect(RSQLite::SQLite(), "airbnb.sqlite")
DBI::dbListTables(bd)

#-------------------------

#EXTRACCION

##EJERCICIO 1. Extracción (listings):

#Crea un data frame de pandas o de R a partir de la tabla listings con las 
#consideraciones que se indican a continuación. Con SQL, haz un join con la 
#tabla hoods para añadir el dato de distrito (neighbourhood_group) y asegúrate 
#de que extraes esta columna en el data frame en lugar de neighbourhood. 

listings_bd <- tbl(bd, sql("
                          SELECT L.id, L.room_type, L.price, 
                          L.number_of_reviews, L.review_scores_rating, 
                          H.neighbourhood_group
                          FROM Listings as L
                          INNER JOIN Hoods as H ON 
                          L.neighbourhood_cleansed = H.neighbourhood
                          "))

listings <- listings_bd %>% collect()

head(listings)

#-------------------------

##EJERCICIO 2. Extracción (reviews):

#Con SQL, haz un join con la tabla hoods para añadir el dato de distrito 
#(neighbourhood_group). También en SQL, cuenta a nivel de distrito y mes el 
#número de reviews. Además, extrae los datos desde 2011 en adelante.

reviews_bd <- tbl(bd, sql("
                         SELECT H.neighbourhood_group, strftime('%Y-%m', 
                         R.date) as mes, COUNT(R.comments) as num_reviews
                         FROM Reviews as R
                         INNER JOIN Listings as L ON R.listing_id = L.id
                         INNER JOIN Hoods as H ON 
                         L.neighbourhood_cleansed = H.neighbourhood
                         WHERE strftime('%Y', R.date) > '2010'
                         GROUP BY H.neighbourhood_group, mes
                         "))

reviews <- reviews_bd %>% collect()

head(reviews)

#-------------------------

#TRANSFORMACION

##EJERCICIO 3. Transformación (listings):

#Antes de realizar la agregación que se pide, tienes que tratar las columnas 
#price,, number_of_reviews y review_scores_rating. Empieza con el precio. 
#Necesitas pasarla a numérica. Ahora mismo es de tipo texto y lo primero que 
#necesitamos es quitar símbolos raros.

listings <- listings %>%
  mutate(price = as.numeric(gsub("[\\$,]", "", price)))

head(listings)

#-------------------------

##EJERCICIO 4. Transformación (listings):

#Transformación (listings). Toca imputar los valores missing de 
#number_of_reviews y review_scores_rating. En este caso, imputa los valores 
#missing con valores reales dentro de la tabla, a nivel de room_type, escogidos 
#de manera aleatoria.

cols <- c("number_of_reviews", "review_scores_rating")

#Bucle para que realice las operaciones en cada una de las columnas
for (j in cols){
  #Bucle para que pase cada fila
  for(i in seq_along(listings[[j]])){
    #Si el valor es NA
    if (is.na(listings[[j]])[i] == TRUE){
      #Calculo lo que vale room_type
      value_rt <- listings$room_type[i]
      #Genero un numero aleatorio en funcion del valor de room_type
      new_value <- sample(na.omit(listings[[j]][
        listings$room_type == value_rt]), 1)
      #Asigno el numero aleatorio
      listings[[j]][i] <- new_value
    }
  }
}

#Compruebo si no hay NAs
any(is.na(listings$number_of_reviews))
any(is.na(listings$review_scores_rating))

head(listings)

#-------------------------

#EJERCICIO 5. Transformación (listings):

#Transformación (listings). Con los missing imputados y el precio en formato 
#numérico ya puedes agregar los datos. A nivel de distrito y de tipo de 
#alojamiento, hay que calcular: Nota media ponderada, Precio mediano y numero 
#de alojamientos

listings_agg <- listings %>% 
  group_by(neighbourhood_group, room_type) %>% 
  #Añado 3 columnas: media ponderada, precio mediano y numero de alojamientos
  summarise(media_ponderada = weighted.mean(review_scores_rating, 
                                            number_of_reviews),
            price_mediana = median(price),
            num_alojam = n())

head(listings_agg)

#-------------------------
  
#EJERCICIO 6. Transformación (reviews):

#Vamos a añadir ahora a simular que tenemos un modelo predictivo y lo vamos a 
#aplicar sobre nuestros datos. Así, la tabla que subamos de nuevo a la base de 
#datos tendrá la predicción añadida. El último mes disponible es julio, así que
#daremos la predicción para agosto. Esto no es una asignatura de predicción de 
#series temporales, así que nos vamos a conformar con tomar el valor de julio 
#como predicción para agosto (a nivel de distrito). Al final, deja el data
#frame ordenado a nivel de distrito y mes.

#Calculo la prediccion filtrando por el mes de julio en el data frame original
#y cambiando la columna de mes para que sea agosto
prediccion_ag <- reviews %>% 
  filter(mes == '2021-07') %>% 
  mutate(mes = '2021-08')

#Añado la prediccion al data frame original
reviews <- bind_rows(reviews, prediccion_ag)

#Ordeno el data frame
reviews <- reviews %>% 
  arrange(neighbourhood_group, mes)

tail(reviews)

#-------------------------

#EJERCICIO 7. Transformación (reviews):

#Hay casos que no tienen dato, por ejemplo, febrero de 2011 en Arganzuela. Como 
#no hay dato, asumiremos que es 0. Siguiendo esta idea, añade todos los 
#registros necesarios a la tabla. 

#Vector con todas las fechas, desde enero 2011 a agosto 2021
fechas <- strftime(seq(as.Date("2011-01-01"), as.Date("2021-08-01"), 
                       by = "month"), '%Y-%m')

#Vector con los distritos
distritos <- unique(reviews$neighbourhood_group)

#Creo un data frame con los vectores anteriores
reviews_agg <- data.frame(fechas, rep(distritos, length(fechas)))
colnames(reviews_agg) <- c("mes", "neighbourhood_group")

#Hago full join para unir los datos del df original por las columnas de
# neighbourhood_group y mes y ordeno las columnas en funcion del mes y distrito
reviews_agg <- reviews_agg %>% 
  full_join(reviews, by=c("neighbourhood_group", "mes")) %>% 
  arrange(neighbourhood_group, mes)

#Como resultado de lo anterior hay NA en la columna num_reviews, los sustituyo 
#por 0
reviews_agg$num_reviews[is.na(reviews_agg$num_reviews)] <- 0

head(reviews_agg)

#-------------------------

#EJERCICIO 8. Carga:

#Sube a la base de datos las dos tablas que has creado. No sobreescibas las
#que hay: crea dos tablas nuevas

#Cargo las tablas
dbWriteTable(bd, "distrib_alojam", listings_agg)
dbWriteTable(bd, "num_reviews_pred", reviews_agg)

#Compruebo
tbl(bd, sql("SELECT * FROM distrib_alojam LIMIT 10"))
tbl(bd, sql("SELECT * FROM num_reviews_pred LIMIT 10"))