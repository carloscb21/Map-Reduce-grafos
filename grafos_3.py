# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

#FUNCIONA EN FLOP 2.2!!
class GrafosA(MRJob):
    """
    Obtenemos el camino de los aritas adyacentes del ultimo al primer valor de la arista

    (A,B) (B, C) ---> (A,B,C)

    SIN REPETICIÓN, y sin que puedan combinarse de varias formas. 
    """
    #Definimos el mapper para leer las lineas y nos quedamos con los valores (A,B)
    def mapper(self, _, line):
        linea_separada = line.replace('"',"")
        linea_separada = linea_separada.split(",")
        #emito todos los valores que hay en las lineas separadas como clave y valor
        yield linea_separada[0], linea_separada[1]
        yield linea_separada[1], linea_separada[0]     
    
    #Primer reducer
    def reducer(self, key, values):
        """        
        emito todos los valores que no sean igual a la clave y que no esten
        en la lista_valores, asi obtengo todas las combinaciones posibles
        sin simetria pero sin repeticiones entre si, decir, solo hay 
        un unico(A,B) pero puede haber (B,A)
        """
        lista_valores = []
        for valor in values:
            if(valor != key) and (not (valor in lista_valores)):
                lista_valores.append(valor)
                #emito como clave nada, y como valor (key,lista_valores[i])
                yield "Nada",(key, valor)
    
    #segundo reducer          
    def reducer_filtro(self,key,values):
        """        
        emito todos los valores como clave una arista y como valor,
        otra arista que, o bien, tienen el comun el elemento los
        algun vertice y hago un filtrado para que no se repitan 
        los valores con claves. Por ejemplo:
        
        Clave  Valor
        [A,B] [B,C]
        [A,B] [C,A]
        [A,C] [C,B]
        [A,C] [D,A]
        """   
        
        #Guardo todas las aristas
        listaDeVertices_DosADos =[]
        for arista in values:
            listaDeVertices_DosADos.append(arista)
        #para cada valor comprueba si comparten algun vertice, y los otros dos, no son iguales
        lista_aux = []
        for valor in listaDeVertices_DosADos:
            #me quedo con los valores de la arista
            pos1_vertice = valor[0]
            pos2_vertice = valor[1]
            listaPosibleCombi = []
            for valorComp in listaDeVertices_DosADos:
                #me quedo con los valores de la arista con la que voy a comparar
                pos1_vertice2 = valorComp[0]
                pos2_vertice2 = valorComp[1]
                
                #aplico las condiciones
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

    #Tercer reducer
    def reducer_sol(self,key,values):
        lista_aux_sa= []
        vertice1_x = key[0]
        vertice1_y = key[1]
        for aristaCombi in values:
            lista_aux_sa.append(aristaCombi)
            if (vertice1_y ==aristaCombi[0])and([aristaCombi[1],vertice1_x] in values):
                listaTriangulo =[vertice1_x,vertice1_y,aristaCombi[1]]
                listaTriangulo.sort()
                yield listaTriangulo,"Nada"

    #Cuarto reducer
    def reducer_sol_final(self,key,values):
        vertice1_x = key[0]
        vertice1_y = key[1]
        vertice1_z = key[2]
        yield (vertice1_x,vertice1_y,vertice1_z),None
        

    
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                    reducer = self.reducer),
            MRStep(reducer = self.reducer_filtro),
            MRStep(reducer = self.reducer_sol),
            MRStep(reducer = self.reducer_sol_final) 
        ]

if __name__ == '__main__':
    import sys
    sys.stderr = open('localerrorlog.txt','w')
    GrafosA.run()
