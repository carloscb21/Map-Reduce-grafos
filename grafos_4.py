# -*- coding: utf-8 -*-

from mrjob.job import MRJob
from mrjob.step import MRStep
import time

#FUNCIONA EN FLOP 2.1
class GrafosA(MRJob):
    """
    Devolvemos las combinaciones sin repeticiones con el grado de cada vertice
    """
    #Definimos una lista global para el primer reducer
    global lista
    lista = []
    
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
        con este reducer filtramos los datos para devolver las aristas con las propiedads:
        -todas las combinaciones sin repeticiones
        -eliminamos ciclos
        -las aristas muestras los nodos ordenados
        """
        
         #Escogemos los valores que no son iguales a las claves y ya han sido introducidos, para evitar repeticiones
        lista_valores = []
        for valor in values:
            if(valor != key) and (not (valor in lista_valores)):
                lista_valores.append(valor)
        
        #si la (clave,valor de la lista de valores) ni su simetrico no esta en la lista global, emitimos como valor, la combinacion de arista 
        for i in range(len(lista_valores)):
            if not((key,lista_valores[i]) in lista) and (not((lista_valores[i]),key) in lista)  :
                lista.append((key,lista_valores[i]))
                yield "hola",(key ,lista_valores[i])
            
    #Segundo reducer
    def reducer_sol(self,key,values):
        """
        con este reducer obtenemos los grados de las combinaciones de aristas
        del primer reducer. Para ello tenemos en cuenta, que por cada vertice
        de la lista de arista, le corresponde un grado, es decir, si aparece 
        una unica vez en el values, sera de grado uno, si aparece dos veces, 
        sera de grado dos.
        """
        #creamos un diccionario para guardar los distintos valores A,B,C,D,...etc
        lista_aristas =[]
        coleccion_aristas_grado = {}
        #Correspondemos a la idea de que por cada vertice, es decir, por cada valor que viene del Values, el grado es 1 para ese valor
        for arista in values:
            #distingos casos
            if not (arista[0] in coleccion_aristas_grado):
                coleccion_aristas_grado[arista[0]]=1
            elif (arista[0] in coleccion_aristas_grado):
                coleccion_aristas_grado[arista[0]]+=1
            if not (arista[1] in coleccion_aristas_grado):
                coleccion_aristas_grado[arista[1]]=1
            elif (arista[1] in coleccion_aristas_grado):
                coleccion_aristas_grado[arista[1]]+=1
            #guardamos la arista
            lista_aristas.append(arista)
        
        #emitimos la arista simplicada del primer reducer como clave, y como valor emitimos los grados calculados en el diccionario
        for arista_sol in lista_aristas:
            yield arista_sol,[coleccion_aristas_grado[arista_sol[0]],coleccion_aristas_grado[arista_sol[1]]]
        
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