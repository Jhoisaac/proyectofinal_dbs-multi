# Curso BigData & Data Analytics by Handytec
# Fecha: Marzo-2016
# Descripcion: Programa que cosecha tweets desde la API de twitter usando tweepy

import couchdb #Libreria de CouchDB (requiere ser instalada primero)
from tweepy import Stream #tweepy es la libreria que trae tweets desde la API de Twitter (requiere ser instalada primero)
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json #Libreria para manejar archivos JSON


###Credenciales de la cuenta de Twitter########################
#Poner aqui las credenciales de su cuenta privada, caso contrario la API bloqueara esta cuenta de ejemplo
ckey = "H18uLz1Knv19pNVn5yC4ZU73g"
csecret = "pCVdPqqf6rB1qEWK6LE8gYXoOEC1QCB77cC84rlcDgGV97yxtR"
atoken = "786303371267280898-Rgk9D4vOxpfVwo8HMORp3m3ds1wj3HF"
asecret = "DplYPb55yFrXhf5mK4pTZ4SZwfqoLFBMJsdXj5E44Y4zg"
#####################################

class listener(StreamListener):
    
    def on_data(self, data):
        dictTweet = json.loads(data)
        try:
            dictTweet["_id"] = str(dictTweet['id'])
            #Antes de guardar el documento puedes realizar parseo, limpieza y cierto analisis o filtrado de datos previo
            #a guardar en documento en la base de datos
            doc = db.save(dictTweet) #Aqui se guarda el tweet en la base de couchDB
            print ("Guardado " + "=> " + dictTweet["_id"])
        except:
            print ("Documento ya existe")
            pass
        return True
    
    def on_error(self, status):
        print (status)
        
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
twitterStream = Stream(auth, listener())

#Setear la URL del servidor de couchDB
server = couchdb.Server('http://localhost:5984/')
try:
    #Si no existe la Base de datos la crea
    db = server.create('pesantez_tweet')
except:
    #Caso contrario solo conectarse a la base existente
    db = server['pesantez_tweet']
    
#Aqui se define el bounding box con los limites geograficos donde recolectar los tweets
twitterStream.filter(track=['@PesantezOficial', '@pesanteztwof', '@UnionEc', '#PesantezPresidente', '#El19Vota19', '#UnionEcuatoriana', '@UnionEc_Guayas', '@UnionEc_Manabi', '@UnionEc_Manta'])
#twitterStream.filter(locations=[-92.6038,-5.0143511,-75.188794,2.2955])
