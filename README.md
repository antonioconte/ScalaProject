# ScalaProject
Progetto di Scalable and Cloud Programming - Università di Bologna A.A. 2018/2019

### Run 
Per eseguire il progetto in local mode su _Spark_ bisogna prima creare il relativo jar

```
sbt assembly         
```
e successivamente
```
spark-submit --class Main /path-jar/HelpfulnessRank-assembly-0.1.jar PARA_LIST 
```

PARAM_LIST:
1. tipo computazione - Product | General
2. demoOn - True | False
3. path input file 
4. path output dir 
5. Lambda (testato con valore 20)
6. Numero di Iterazioni (testato con valore 10) 
7. Numero di partizioni
8. Timeout (ms) tra due iterazioni successive (se demoOn = True)

####### Esempio di Run
```
spark-submit --class Main ./target/scala-2.12/HelpfulnessRank-assembly-0.1.jar \
          Product True dataset/demo.csv result 20 10 7 3000
 ```
