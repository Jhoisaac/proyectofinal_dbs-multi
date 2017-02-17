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
ckey = "WkTjYXSOvVxVVbNZYfWoERniH"
csecret = "pqih7MBM6L6h513czy69scb3VJJA23TG85A7XpdsXPgcxf0BPE"
atoken = "786303371267280898-8JnQ9hubaEUPZhoEmeSMHbJlzujPJ7Y"
asecret = "ecE87gKDk6ZAssV8kiQPq2mAeTFCGSf9hzqYtbwJQHIeu"
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
    db = server.create('muro_mexico')
except:
    #Caso contrario solo conectarse a la base existente
    db = server['muro_mexico']
    
#Aqui se define el bounding box con los limites geograficos donde recolectar los tweets
twitterStream.filter(track=['el muro en la Frontera con EEUU', '#Muro', '#muro', 'el muro de Trump', 'el Muro con Mexico', 'el muro'])
#twitterStream.filter(locations=[-102.1159,21.6161,-97.8947,23.3635])
