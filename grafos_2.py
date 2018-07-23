# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

#FUNCIONA EN FLOP 2.2!!
class GrafosA(MRJob):
    """
    Obtenemos el camino de los aritas adyacentes del ultimo al primer valor de la arista

    (A,B) (B, C) ---> (A,B,C)

    SIN REPETICIÓN
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

    def reducer_sol(self,key,values):
        listaDeVertices =[]
        for arista in values:
            listaDeVertices.append(arista[0])
            listaDeVertices.append(arista[1])
        lista_triangulos = []
        pos1 = 0
        longitud = len(listaDeVertices)/2
        while pos1<longitud:
            pos1_vertice = listaDeVertices[pos1*2]
            pos2_vertice = listaDeVertices[(pos1*2)+1]     
            pos2 = 0
            while pos2<longitud:
                pos1_vertice2 = listaDeVertices[pos2*2]
                pos2_vertice2 = listaDeVertices[(pos2*2)+1]
                if (pos2_vertice ==pos1_vertice2)and(pos2_vertice2!=pos1_vertice):
                    pos3 = 0
                    while pos3<longitud:
                        pos1_vertice3 = listaDeVertices[pos3*2]
                        pos2_vertice3 = listaDeVertices[(pos3*2)+1]
                        if(pos2_vertice2==pos1_vertice3) and (pos1_vertice ==pos2_vertice3):
                            lista_aux = []
                            if (len(lista_triangulos)>0):
                                for i in lista_triangulos:
                                    condicion1 = pos1_vertice in i
                                    condicion2 = pos1_vertice2 in i
                                    condicion3 = pos1_vertice3 in i
                                    condicion = condicion1 and condicion2 and condicion3 
                                    lista_aux.append(condicion)
                                if True in lista_aux:
                                    pass
                                else:
                                    lista_triangulos.append((pos1_vertice,pos1_vertice2,pos1_vertice3))
                            elif(len(lista_triangulos)==0):
                                lista_triangulos.append((pos1_vertice,pos1_vertice2,pos1_vertice3))

                        pos3 = pos3+1
                pos2 = pos2 +1
            
            pos1 = pos1+1
        for lista_triangulos_salida in lista_triangulos:
            #no hacemos sort()
            yield lista_triangulos_salida, None


    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(reducer = self.reducer_sol) 
        ]

if __name__ == '__main__':
    import sys
    sys.stderr = open('localerrorlog.txt','w')
    GrafosA.run()
    
