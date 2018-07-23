# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import time


class GrafosA(MRJob):
    """
    Obtenemos el camino de los aritas adyacentes del ultimo al primer valor de la arista

    (A,B) (B, C) ---> (A,B,C)

    CON REPETICIÓN y unicamente enviamos el ultimo que encontramos
    """

    def mapper(self, _, line):
        """
        Enviamos los datos como:

        clave A y valor B
        clave B y valor A
        """
        linea_separada = line.replace('"',"")
        linea_separada = linea_separada.split(",")
        yield linea_separada[0], linea_separada[1]
        yield linea_separada[1], linea_separada[0]
        
            
    def reducer(self, key, values):
        """
        Nos da la primera combinacion
        """
        lista_valores = []
        for valor in values:
            if(valor != key) and (not (valor in lista_valores)):
                lista_valores.append(valor)
                yield "kkey",(key, valor)


    def reducer_filtro(self,key,values):
        listaDeVertices_DosADos =[]
        for arista in values:
            listaDeVertices_DosADos.append(arista)

        lista_aux = []
        for valor in listaDeVertices_DosADos:
            pos1_vertice = valor[0]
            pos2_vertice = valor[1]
            listaPosibleCombi = []
            for valorComp in listaDeVertices_DosADos:
                pos1_vertice2 = valorComp[0]
                pos2_vertice2 = valorComp[1]

                if (pos1_vertice==pos2_vertice2) and(pos2_vertice!=pos1_vertice2)and(not(valorComp in listaPosibleCombi)):
                    listaPosibleCombi.append(valorComp)
                    lista_aux.append(valorComp)
                    if (not valor in lista_aux) and (lista_aux !=[]):
                        yield valor, valorComp
                if (pos2_vertice ==pos1_vertice2)and(pos2_vertice2!=pos1_vertice) and(not(valorComp in listaPosibleCombi)):
                    listaPosibleCombi.append(valorComp)
                    lista_aux.append(valorComp)
                    if (not valor in lista_aux) and (lista_aux !=[]):
                        yield valor, valorComp
                    

    def reducer_sol(self,key,values):
        """
        Tenemos todas las posibilidades de la combinacion de las letras
        """
        vertice1_x = key[0]
        vertice1_y = key[1]
        for aristaCombi in values:
            if (vertice1_y ==aristaCombi[0]):
                listaTriangulo =[vertice1_x,vertice1_y,aristaCombi[1]]
                listaTriangulo.sort()
        yield listaTriangulo, "miau :)"

    
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(reducer = self.reducer_filtro),
            MRStep(reducer = self.reducer_sol) 
        ]

if __name__ == '__main__':
    import sys
    sys.stderr = open('localerrorlog.txt','w')
    GrafosA.run()
