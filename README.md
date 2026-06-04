# AML Classifier — Pipeline Big Data

Trabalho prático da cadeira de Bases de Dados (BD) — recolha e processamento de
dados na plataforma big data (HDFS, Spark, Kafka, Hive). Implementa um
classificador *Anti-Money Laundering* sobre o dataset sintético **IBM AML**,
combinando treino batch e classificação em streaming.

## Arquitetura

```
              ┌─ OFFLINE (batch) ─────────────────────────────────┐
              │  Small CSV ──► Trainer ──► PipelineModel (HDFS)   │
              └────────────────────────────────┬──────────────────┘
                                               │ load
              ┌──── ONLINE (streaming) ────────┴──────────────────┐
              │                                                   │
  Medium CSV ─► Producer ──► Kafka ──► Consumer (Spark Streaming) │
                                       (model.transform)          │
                                              │                   │
                                              ▼                   │
                                       predictions/ (parquet)     │
              ┌───────────────────────────────────────────────────┘
              │ OFFLINE (batch SQL / Hive)
              │  predictions/ + dim_accounts ──► Stats Processor ──► KPIs
              └───────────────────────────────────────────────────
```

## Pipeline — passo a passo

1. **Dados em HDFS.** `LI-Small_Trans.csv` (treino) e `LI-Medium_Trans.csv`
   (simulação de stream) em `/aulas/fabricio_moreira/project/dataset/`.

2. **Tabela de dimensão (multi-fonte).** `aml_stats.ipynb` constrói
   `dim_accounts` a partir de `LI-Medium_accounts.csv` (o stream consome só
   o Medium, portanto Small não acrescenta nada aos joins), deduplicada por
   `(bank_id, account_number)` e com `entity_type` derivado por regex sobre
   `entity_name`. Persiste em `/dim/accounts/` como parquet. É a segunda
   fonte que se cruza com a tabela de factos das transações.

3. **Treino offline batch (Spark MLlib).** `aml_trainer.ipynb` lê o Small, faz
   feature engineering (log dos montantes, diferença, flags `same_bank` /
   `ccy_mismatch`, hora) e treina um `RandomForest` num `Pipeline` com indexers
   + one-hot + assembler, usando **pesos de classe** para o forte
   desbalanceamento (laundering &lt;1%). Avalia AUC, F1 e matriz de confusão.

4. **Modelo persistido em HDFS.** O `PipelineModel` é gravado em
   `/model/rf_aml_pipeline/`, disponível para o consumer carregar com
   `PipelineModel.load(...)`.

5. **Producer envia transações em streaming simulado.**
   `aml_kafka_producer.ipynb` lê o Medium, serializa cada linha em JSON e envia
   em batches para o tópico Kafka `transactions`, com `sleep` entre batches
   para simular a chegada no tempo.

6. **Camada de transporte:** Kafka (broker) entre producer e consumer.

7. **Consumer recebe + classifica num único job.** `aml_kafka_consumer.ipynb`
   é um job Spark Structured Streaming que lê do tópico, replica a feature
   engineering do trainer e chama `model.transform(df)`. **O modelo está
   embutido na pipeline**, não é um serviço externo — `transform` devolve um
   DataFrame já com `prediction` e `prob_fraud`.

8. **Consumer mostra no console** com coluna `status`:
   `"BLOQUEADA - suspeita de fraude"` ou `"OK - fidedigna"`, junto com
   `From Bank`, `To Bank`, `From Account`, `Amount Paid` e `prob_fraud`.

9. **Consumer grava resultados em Parquet** particionado por hora de ingestão
   em `/stream/predictions_kafka/`. Em paralelo escreve janelas de 30s com
   contagens em `/stream/metrics_kafka/` para o benchmark.

10. **Processor batch — KPIs e SQL ad-hoc (últimos 7 dias).** `aml_stats.ipynb`
    lê as previsões, **filtra à janela dos últimos 7 dias por `ingest_ts`**
    (ingest time, relativa a `current_timestamp()` — distinto de `Timestamp`
    que é event time fixo no dataset de 2022), junta com `dim_accounts` e
    produz: total de transações, fidedignas vs suspeitas (n e %), valor total
    e valor suspeito, fraude por banco real, fraude por tipo de entidade
    (PF vs PJ), top contas suspeitas. Por fim regista as tabelas externas
    Hive `aml_dim_accounts` e `aml_predictions` (base de dados `default`)
    para queries SQL puras — fecha o requisito "Spark, MapReduce, Hive, ..."
    do enunciado.

11. **Benchmark comparativo.** `aml_benchmark.ipynb` mede qualidade do modelo
    (accuracy / precision / recall / F1), throughput do consumer streaming
    (msgs/s a partir das janelas de métricas) e o tempo batch de leitura
    Small vs Medium para mostrar o efeito do volume.

## Notebooks

| Notebook                       | Tipo               | Papel                                         |
| ------------------------------ | ------------------ | --------------------------------------------- |
| `aml_trainer.ipynb`            | batch              | Treino offline do `PipelineModel` em HDFS     |
| `aml_kafka_producer.ipynb`     | streaming producer | Lê Medium CSV → JSON → Kafka                  |
| `aml_kafka_consumer.ipynb`     | streaming consumer | Kafka → modelo → parquet de previsões         |
| `aml_stats.ipynb`              | batch              | `dim_accounts`, KPIs 7d, tabelas Hive externas |
| `aml_benchmark.ipynb`          | batch              | Qualidade, throughput, comparação Small/Medium |
| `aml_auto_trainer.py`          | serviço (loop)     | Retreino automático/periódico do modelo em HDFS |

## Treino automático e periódico

Para manter o modelo atualizado sem o correr à mão, o `aml_trainer.ipynb` continua
a ser a única implementação do algoritmo de treino. Antes de executar, os
notebooks são convertidos para `.py` (`make ipynb2py`); o `aml_auto_trainer.py`
apenas decide quando chamar o `aml_trainer.py` gerado.

1. **Producer com dual-write.** `aml_kafka_producer.ipynb` passa a gravar cada
   batch em HDFS (`training/incremental/`, Parquet com label) **além** de o
   enviar para Kafka. É a fonte de dados novos para o retreino.

2. **Auto-trainer.** `aml_auto_trainer.py` faz polling de
   `training/incremental/` e, quando acumulam `RETRAIN_THRESHOLD` (default 5000)
   novas linhas desde o último treino, chama `spark-submit aml_trainer.py` com
   `AML_INCLUDE_INCREMENTAL=1`. O `aml_trainer.py` gerado pelo notebook retreina
   com o dataset base (`Small`) + todos os incrementais. O polling lê apenas um contador
   (`training/manifests/_counter.json`, atualizado pelo producer a cada batch) —
   evita um `spark.count()` sobre todo o Parquet a cada ciclo (custo O(1), sem
   jobs Spark e sem o ruído de `TaskKilled`/broadcast).

3. **Publicação atómica e versionada.** O modelo é gravado primeiro em
   `model/versions/rf_aml_pipeline_<timestamp>/`, depois atualiza o
   `model/rf_aml_pipeline` (legacy) e só no fim publica o ponteiro
   `model/active_model.json` (path + métricas + contagens). Evita que o consumer
   carregue um modelo a meio da escrita.

4. **Consumer com hot-reload.** `aml_kafka_consumer.ipynb` usa `foreachBatch` e,
   a cada micro-batch, lê `active_model.json` e recarrega o `PipelineModel` só se
   a versão mudou — as previsões passam a usar o modelo mais recente sem reiniciar
   o job.

Correr (serviço de longa duração, parar com Ctrl-C):

```
make auto-trainer        # converte notebooks e executa python aml_auto_trainer.py
```

Parâmetros (env ou `config`): `AML_RETRAIN_THRESHOLD`, `AML_POLL_INTERVAL`
(default 10s), `AML_HDFS_BASE`, `AML_SPARK_MASTER`.

### Comportamento durante o retreino

Enquanto o `aml_trainer.py` chamado pelo `aml_auto_trainer.py` está a treinar um
novo modelo, o sistema não descarta dados:

- O producer continua a enviar mensagens para Kafka e a gravar os mesmos batches
  em `training/incremental/`.
- O consumer continua ativo e classifica os micro-batches com o modelo atualmente
  publicado.
- O novo modelo só fica visível depois de o treino terminar e o ficheiro
  `model/active_model.json` ser atualizado.

O retreino usa o dataset base (`LI-Small_Trans.csv`) mais os dados incrementais
que o Spark encontra em `training/incremental/` no momento da leitura. Dados que
chegam enquanto o treino já está a decorrer ficam persistidos em HDFS e serão
considerados no retreino seguinte. Pode existir uma pequena diferença temporal
entre o contador lido antes do treino e os ficheiros Parquet efetivamente lidos,
mas isso não perde dados; no pior caso algumas linhas entram num treino e ainda
contam para o próximo ciclo.

### Hot-reload do modelo no consumer

O consumer usa `foreachBatch`. Em cada micro-batch chama `get_active_model()`:

- lê `model/active_model.json` em HDFS;
- compara o `model_path` publicado com o modelo atualmente em memória;
- se o path mudou, carrega o novo `PipelineModel`;
- se o path não mudou, reutiliza o modelo já carregado.

A troca de modelo acontece entre micro-batches, nunca a meio de um batch. Por
exemplo: se o batch 20 começou com o modelo A e o auto-trainer publica o modelo
B durante esse processamento, o batch 20 termina com o modelo A; o batch 21 lê o
novo `active_model.json`, carrega o modelo B e passa a usá-lo.

## Mapeamento à proposta inicial

| Passo proposto                                | Implementação                          | Notas                                     |
| --------------------------------------------- | -------------------------------------- | ----------------------------------------- |
| 1. Dados em HDFS                              | HDFS                                   | ✓                                         |
| 2. Treino do modelo no Small                  | `aml_trainer.ipynb`                    | ✓                                         |
| 3. Modelo disponível                          | `model/` em HDFS                       | ✓                                         |
| 4. Producer envia transações                  | `aml_kafka_producer.ipynb`             | ✓ em batches, não row-by-row              |
| 5. Camada de transporte                       | Kafka                                  | broker entre producer e consumer          |
| 6 + 7. Consumer recebe e classifica           | `aml_kafka_consumer.ipynb`             | fundidos num único Spark job              |
| 8. Print de "fidedigna / suspeita"            | sink consola com coluna `status`       | ✓                                         |
| 9. Resultados em ficheiro                     | `predictions_kafka/` em parquet        | ✓ não CSV (parquet é colunar / Hive-able) |
| 10. Estatísticas a partir dos resultados      | `aml_stats.ipynb`                      | ✓ + `dim_accounts` + tabelas Hive         |

## Como correr (ordem para a defesa)

1. **`aml_trainer.ipynb`** — uma vez, treino offline (baseline inicial).
2. Janelas separadas (deixar consumer arrancar primeiro):
   - `aml_kafka_consumer.ipynb` → depois `aml_kafka_producer.ipynb`
3. **`aml_auto_trainer.py`** (`make auto-trainer`) — opcional, em paralelo:
   retreina o modelo automaticamente à medida que o producer alimenta o HDFS.
4. **`aml_stats.ipynb`** — KPIs de negócio + tabelas Hive.
5. **`aml_benchmark.ipynb`** — qualidade + throughput + Small vs Medium.

## Configuração

Parâmetros definidos como variáveis no topo de cada notebook:

- `HDFS_BASE` — namenode + diretório do projeto.
- `KAFKA_BOOTSTRAP` e `KAFKA_TOPIC` — nos dois notebooks Kafka.

O conector `spark-sql-kafka-0-10` é assumido como disponível no classpath do
cluster Spark (instalação típica em `$SPARK_HOME/jars/`).
