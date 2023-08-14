library(dplyr)
library(stringr)

#Escribe un código de R o Python para validar las contraseñas que tienes más 
#abajo de acuerdo a las siguientes condiciones:
  
# - Tienen que contener entre 8 y 20 caracteres (ni más, ni menos)
# - Debe contener al menos una mayúscula, una minúscula y un número
# - tienen que contener al menos uno de estos símbolos: !#&

#Creo el dataframe con las contraseñas a comprobar
df <- data.frame("password" = c("Apple34!rose", "My87hou#4$", "abc123abc"))

#Diseño una funcion con expresiones regulares para comprobar las contraseñas

#El primer ifelse con str_detect busca que cumpla con los criterios(una 
#mayuscula, un caracteres especial, una minuscula, un numero y una longitud
#entre 8 y 20
#Encadeno otro ifelse que si cumple lo anterior suprime los caracteres ya 
#mencionados y comprueba la longitud resultante. Si es igual a 0 significa 
#que no hay ningun caracter que incumpla los criterios, es valida

password_validation <- function(password){
  ifelse((str_detect(password, 
                     regex("^(?=.*[A-Z])(?=.*[!#&])(?=.*[a-z])(?=.*[0-9]).{8,20}$"))), 
         ifelse((str_length(str_replace_all(password, regex("[a-zA-z0-9!#&]"), ""))) == 0, TRUE, FALSE),FALSE
  )
}

#Con dplyr creo una nueva columna que sea la comprobacion

df <- df %>% 
  mutate(comprobacion = password_validation(password))

print(df)

#La primera cumple los criterios, la segunda los incumple por el "$" y la
#ultima por la falta de mayusculas y de caracter especial