# Map e Reduce

## Atividade 1
Disciplina de Programação com Map e Reduce, ministrada pelo prof. Robério no curso de Pós em Ciência de Dados, na UNI7. Primeira [atividade](dataset\uni7-pod-map-reduce-exercise-01.pdf) da disciplina.

Requerimentos
* `Python 3.6`
* `mrjob`

### Run

Para rodar, irei supor que o user já possui o python instalado em máquina e com o `pip` setado na variável de ambiente.

#### Primeiro passo

Baixar as packages do MRjob:

```console
user@computer:~$ pip install mrjob
```
#### Segundo passo

Para rodar um sript, você deve executar o seguite comando:

```console
user@computer:~$ python <script>.py dataset/<file>.txt >> output/<out>.txt
```

Por exemplo, vamos executar o script que resolve o exercício 2:       


```console
user@computer:~$ python MRPopularHero.py dataset/Marvel-graph.txt >> output/hero.txt
```
## Atividade 2

```console
user@computer:~$ python RecommendationFriends.py dataset/Marvel-graph.txt >> output/recomendations.txt
```

Para mais detalhes dos métodos do MRjob:
> Documentação [MRJob](https://pythonhosted.org/mrjob/job.html).

Equipe:
* Igor Farias
* Pedro Andrade
