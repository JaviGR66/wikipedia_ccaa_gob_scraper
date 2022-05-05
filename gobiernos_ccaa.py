import requests
import json

from neo4j import GraphDatabase
from bs4 import BeautifulSoup
import time

class insert_neo(object):
    neo_connection = None
    list_ccaa = ['Canarias', 'Principado de Asturias', 'Comunidad Valenciana', 'Región de Murcia',
                 'Galicia', 'Comunidad de Madrid', 'Castilla y León', 'Cantabria', 'Ceuta', 'Cataluña',
                 'País Vasco', 'La Rioja', 'Islas Baleares', 'Melilla', 'Castilla-La Mancha', 'Aragón',
                 'Comunidad Foral de Navarra', 'Extremadura', 'Andalucía']

    def __init__(self, neo_driver, datos):
        self.neo_connection = neo_driver.session()
        self.afiliado = datos['afiliado']
        self.desde = datos["fecha_inicial"]
        self.hasta = datos["fecha_final"]
        self.partido = datos['partido']
        self.comunidad_aut = datos['comunidad_aut']
        self.create_gobiernos()

    def create_gobiernos(self):

        query_par = 'MERGE (pa:Partido {name:"' + self.partido + '"}) RETURN pa'
        self.neo_connection.run(query_par)

        # query_pre = 'CREATE (pr:afiliado {name:"' + self.afiliado + '"}) RETURN pr'
        # self.neo_connection.run(query_pre)

        query_ca = 'MERGE (ca:CCAA {name:"' + self.comunidad_aut + '"}) RETURN ca'
        self.neo_connection.run(query_ca)

        query_rel_ca_gob = 'MATCH (pa:Partido), (ca:CCAA) WHERE pa.name = "' + self.partido + '" ' \
                             'AND ca.name = "' + self.comunidad_aut + '" MERGE (pa)-[r:Gobierna {' \
                             'afiliado:"' + self.afiliado + '",desde:"' + self.desde[0] + '",' \
                             'hasta:"' + self.hasta[0] + '"}]->(ca) RETURN r'
        self.neo_connection.run(query_rel_ca_gob)

        if len(self.desde) == 2:
            query_rel_ca_gob = 'MATCH (pa:Partido), (ca:CCAA) WHERE pa.name = "' + self.partido + '" ' \
                               'AND ca.name = "' + self.comunidad_aut + '" MERGE (pa)-[r:Gobierna {' \
                               'afiliado:"' + self.afiliado + '",desde:"' + self.desde[1] + '",' \
                               'hasta:"' + self.hasta[1] + '"}]->(ca) RETURN r'
            self.neo_connection.run(query_rel_ca_gob)
        # for year in self.years:
        #     query_anio = 'CREATE (an:Anio {name:"' + str(year) + '"}) RETURN an'
        #     self.neo_connection.run(query_anio)
        #
        #     query_rel_anio_gob = 'MATCH (gob:Partido), (an:Anio) WHERE an.name = "' + str(year) + '" ' \
        #                          'AND gob.name = "' + self.partido + '" MERGE (an)-[r:Goberno]->(gob) RETURN r'
        #     self.neo_connection.run(query_rel_anio_gob)
        #
        #     query_rel_anio_pre = 'MATCH (pr:Presidente), (an:Anio) WHERE an.name = "' + str(year) + '" ' \
        #                          'AND pr.name = "' + self.partido + '" MERGE (an)-[r:Persona]->(gob) RETURN r'
        #     self.neo_connection.run(query_rel_anio_pre)
        #
        # query_rel_gob_pre = 'MATCH (gob:Partido), (ca:CCAA) WHERE ca.name = "' + self.comunidad_aut + '" ' \
        #                       'AND gob.name = "' + self.partido + '" MERGE (ca)-[r:Gobernada]->(gob) RETURN r'
        # self.neo_connection.run(query_rel_gob_pre)
        #
        # query_rel_gob_ca = 'MATCH (gob:Partido), (pre:Presidente) WHERE pre.name = "' + self.presidente + '" ' \
        #                      'AND gob.name = "' + self.partido + '" MERGE (gob)-[r:Presidido]->(pre) RETURN r'
        # self.neo_connection.run(query_rel_gob_ca)


class BDDD_Conection(object):
    neo_connection = None

    def __init__(self, neo_driver):
        self.neo_connection = neo_driver.session()

    def delete_database(self):
        self.neo_connection.run("MATCH (n) DETACH DELETE n")


neo_driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
# BDDD_Conection(neo_driver).delete_database()
# PETICION
with requests.Session() as session:
    response = session.get('https://es.'
                           'wikipedia.org/wiki/Anexo:Presidencias_de_las_comunidades_aut%C3%B3nomas_espa%C3%B1olas')
    soup = BeautifulSoup(response.content, "html.parser")
    raw_data = soup.find_all('table')[3]

# print(raw_data)
# list_ccaa = ['Canarias', 'Principado de Asturias', 'Comunidad Valenciana', 'Región de Murcia', 'Galicia',
#              'Comunidad de Madrid', 'Castilla y León', 'Cantabria', 'Ceuta', 'Cataluña', 'País Vasco', 'La Rioja',
#              'Islas Baleares', 'Melilla', 'Castilla-La Mancha', 'Aragón', 'Comunidad Foral de Navarra', 'Extremadura',
#              'Andalucía']

    json_data = {}
    test = False
    for tr in raw_data.findAll("tr"):
        years = []
        cont = 0
        print("--------------------------------------")
        for td in tr.findAll("td"):
            if cont == 0:
                presidente = td.text.strip()
                # print(presidente)
                json_data["afiliado"] = presidente
            if cont == 1:
                fecha_inicial = []
                if len(td.text.split("de ")) > 3:
                    fecha_inicial.append(td.text.split("de ")[2].strip()[0:4])
                    fecha_inicial.append(td.text.split("de ")[4].strip()[0:4])
                else:
                    fecha_inicial.append(td.text.split("de ")[2].strip()[0:4])
                json_data["fecha_inicial"] = fecha_inicial
                # print("fecha inicial ",fecha_inicial)
            if cont == 2:
                fecha_final = []
                # print("fecha final ",td.text)
                if len(td.text.split("de ")) > 3:
                    fecha_final.append(td.text.split("de ")[2].strip()[0:4])
                    if len(td.text.split("de ")) != 5:
                        fecha_final.append("2022")
                    else:
                        fecha_final.append(td.text.split("de ")[4].strip()[0:4])
                else:
                    if td.text == "":
                        fecha_final.append("2022")
                    else:
                        fecha_final.append(td.text.split("de ")[2].strip()[0:4])
                    if len(fecha_inicial) == 2 and len(fecha_final) != 2:
                        fecha_final.append("2022")

                json_data["fecha_final"] = fecha_final
            if cont == 4:
                partido = td.text.strip()
                # print(partido)
                json_data["partido"] = partido
            if cont == 5:
                comunidad_aut = td.text.strip()
                # print(comunidad_aut)
                json_data["comunidad_aut"] = comunidad_aut

            cont += 1

        if json_data != {}:
            if json_data["afiliado"] == 'Patxi López':
                test = True
            if test:
                print(json_data)
                # exit()
                insert_neo(neo_driver, json_data)
                # time.sleep(2.4)

    print("FIN")