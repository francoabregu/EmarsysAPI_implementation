import http.client
import pandas as pd
import json
import unicodedata
import os

class EmarsysAPI:
    def __init__(self, cliente):
        self.getAccess(cliente)
        os.system("node auth.js " + self.user + " " + self.secret)
        with open('x-wsse.json') as file:
            x_wsse = json.load(file)
        self.conn = http.client.HTTPSConnection("api.emarsys.net")
        self.headers = {'X-WSSE': "UsernameToken Username=\"" + self.user + "\", PasswordDigest=\""+ x_wsse["digest"] +"\", Nonce=\""+ x_wsse["nonce"] + "\", Created=\""+ x_wsse["created"] +"\"" ,'content-type': 'application/json'}
        print(self.headers)
    
    def createContacts(self,contactos):
	    print("Creating contacts")
	    payload = "{\"key_id\":\"3\",\"contacts\":[" + contactos + "]}"
	    print(payload)
	    self.conn.request("POST", "/api/v2/contact", payload, self.headers)
	    res = self.conn.getresponse()
	    data = res.read()
	    print(data.decode("utf-8"))
	    return
    
    def updateContacts(self,contactos):
	    print("Updating contacts")
	    payload = "{\"key_id\":\"3\",\"contacts\":[" + contactos + "]}"
	    self.conn.request("PUT", "/api/v2/contact", payload, self.headers)
	    res = self.conn.getresponse()
	    data = res.read()
	    print(data.decode("utf-8"))
	    return
    
    def getContactData(self,emails,campos):
        print("get contact")
        if campos == "":
            payload = "{\"keyId\":\"3\",\"keyValues\":[" + emails + "],\"fields\":[\"1\",\"2\",\"3\"]}"
        else:
            payload = "{\"keyId\":\"3\",\"keyValues\":[" + emails + "],\"fields\":[\"1\",\"2\",\"3\"," + campos + "]}"
        print(payload)
        self.conn.request("POST", "/api/v2/contact/getdata", payload, self.headers)
        res = self.conn.getresponse()
        data = res.read().decode("utf-8")
        print(data)
        return json.loads(data)

    def partir(self,seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    def procesarReporte(self,reporte_path,procesarGrupo):
        df = pd.read_csv(filepath_or_buffer=reporte_path)
        grupos = self.partir(df,1000)
        for grupo in grupos:
            procesarGrupo(grupo)
        return

    def eliminar_tildes(self,text):
        try:
            text = unicode(text, 'utf-8')
        except (TypeError, NameError): # unicode is a default on python 3 
            pass
        text = unicodedata.normalize('NFD', text)
        text = text.encode('ascii', 'ignore')
        text = text.decode("utf-8")
        return str(text)
    
    def getAccess(self, cliente):
        with open('Emarsys_API_Users.json') as f:
            emarsys_api_users = json.load(f)
        self.user = emarsys_api_users[cliente]["user"]
        self.secret = emarsys_api_users[cliente]["secret"]
        return

    def getContactList(self):
        payload = "{}"
        self.conn.request("GET", "/api/v2/contactlist", payload, self.headers)
        res = self.conn.getresponse()
        data = res.read()
        print(data.decode("utf-8"))        

    def createContactList(self,name,description):
        payload = "{\"key_id\":\"3\",\"name\":\"" + name + "\",\"description\":\"" + description + "\",\"external_ids\":[]}"
        print(payload)
        self.conn.request("POST", "/api/v2/contactlist", payload, self.headers)
        res = self.conn.getresponse()
        data = res.read()
        print(data.decode("utf-8"))