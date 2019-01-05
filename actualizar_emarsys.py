import http.client
import sys 
import os
import datetime
import pandas as pd
import json
from multiprocessing import Pool
import re
import unicodedata
from functools import partial
from EmarsysAPI import EmarsysAPI

cliente = sys.argv[1] 
emarsysAPI = EmarsysAPI(cliente)

def get_reporte_path():
	return sys.argv[2]

def generarPayloadCreate(fila):
	nombre = emarsysAPI.eliminar_tildes(str(fila.iloc[0]["s_customer_name"]))
	cliente_mail = str(fila.iloc[0]['email'])
	pago_banco = str(fila.iloc[0]['s_bank'])
	pago_tarjeta = str(fila.iloc[0]['s_credit_card'])
	campos = []
	if(nombre.lower() != "nan"):
		campos.append("\"1\":\"" + nombre.lower().title() + "\"")
	if(pago_banco.lower() != "nan"):
		campos.append("\"7015\":\"" + pago_banco + "\"")
	if(pago_tarjeta.lower() != "nan"):
		campos.append("\"7404\":\"" + pago_tarjeta + "\"")
	if(len(campos) >= 1):
		return "{" + "\"3\":\"" + cliente_mail.lower() + "\"," + "\"31\":\"1\"," + ",".join(campos) + "}"
	else:
		return ""

def generarPayloadUpdate(contacto,fila):
	atributos = []
	nombre = emarsysAPI.eliminar_tildes(str(fila.iloc[0]['s_customer_name']))
	pago_banco = str(fila.iloc[0]['s_bank'])
	pago_tarjeta = str(fila.iloc[0]['s_credit_card'])
	if(contacto["1"] == None and nombre.lower() != "nan"):
		atributos.append("\"1\":\"" + nombre.lower().title() + "\"" ) 
	if(contacto["7015"] == None and pago_banco.lower() != "nan"):
		atributos.append("\"7015\":\"" + pago_banco + "\"" ) 
	if(contacto["7404"] == None and pago_tarjeta.lower() != "nan"):
		atributos.append("\"7404\":\"" + pago_tarjeta.lower().title() + "\"" )
	if(len(atributos) > 0):
		atributos.append("\"3\":\"" + str(fila.iloc[0]['email']) + "\"")
		return "{" + ",".join(atributos) + "}"
	else:
		return ""

def procesarGrupo(df):
	print("procesando grupo")
	emails = []
	contactos_a_crear = []
	contactos_a_actualizar = []
	for index_mail, fila in df.iterrows():
		emails.append("\"" + str(fila['email']) + "\"")
	respuesta = emarsysAPI.getContactData(",".join(emails),"\"7015\",\"7404\"")
	contactos_faltantes = respuesta["data"]["errors"]
	contactos_encontrados = respuesta["data"]["result"] 
	for contacto in contactos_faltantes:
		mail = contacto["key"]
		fila = df.loc[df['email'].str.lower() == mail.lower()]
		contactos_a_crear.append(generarPayloadCreate(fila))
	for contacto in contactos_encontrados:
		mail = contacto["3"]
		fila = df.loc[df['email'].str.lower() == mail.lower()]
		if(fila.shape[0] > 0):
			payloadUpdate = generarPayloadUpdate(contacto,fila)
		else:
			print("Warning: " + mail + " no se encuentra en el csv")		
		if(payloadUpdate != ""):
			contactos_a_actualizar.append(payloadUpdate)
	if(len(contactos_a_crear) > 0):
		emarsysAPI.createContacts(",".join(contactos_a_crear))
	else:
		print("no hay contactos a crear")
	if(len(contactos_a_actualizar) >0):
		emarsysAPI.updateContacts(",".join(contactos_a_actualizar))
	else:
		print("no hay contactos a actualizar")
	print("fin")
	return

emarsysAPI.procesarReporte(get_reporte_path(),procesarGrupo)