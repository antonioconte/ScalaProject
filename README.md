# ScalaProject
Progetto di Scalable and Cloud Programming - Università di Bologna A.A. 2018/2019

Lo scopo di tale lavoro è quello di realizzare un programma che, preso in input un dataset contenente dati relativi a commenti di prodotti su Amazon, determina un rank degli utenti modificando opportunamente la loro _Helpfulness_. Il criterio utilizzato per la modifica della _Helpfulness_ è il seguente: un utente vede aumentare la propria Helpfulness se ha dato un voto, ad un prodotto, uguale a quello di un altro utente con _Helpfulness_ maggiore.
Sono stati realizzati due moduli: uno che segue l'approccio del _PageRank_ nativo (**General**) e l'altro realizzato ad hoc (**Product**) per l'obiettivo di questo progetto. 

### Run 
Per eseguire il progetto in local mode su _Spark_ bisogna prima creare il relativo jar

```
sbt assembly         
```
e successivamente
```
spark-submit --class Main /path-jar/HelpfulnessRank-assembly-0.1.jar PARA_LIST 
```

#### PARAM_LIST:
1. tipo computazione - Product | General
2. demoOn - True | False
3. path input file 
4. path output dir 
5. Lambda (testato con valore 20)
6. Numero di Iterazioni (testato con valore 10) 
7. Numero di partizioni
8. Timeout (ms) tra due iterazioni successive (se demoOn = True)

#### Esempio di Run
```
spark-submit --class Main ./target/scala-2.12/HelpfulnessRank-assembly-0.1.jar \
          Product True dataInput/demo.csv result 20 10 7 3000
 ```
 
 ## DEMO
 La demo è stata sviluppata nell'ambiente NodeJS (server-side) e D3.js per la view del grafo.
 Per il run dell'applicazione posizionarsi all'interno del directory _DEMO_ e lanciare il comando
 ```
 node start
 ```
 e successivamente aprire un broswer web all'url http://localhost:3000
