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

Em paralelo ao caminho Kafka existe um caminho **file-based** (HDFS folder
streaming) que partilha o mesmo modelo e os mesmos sinks — usado para a
**análise comparativa de desempenho** exigida pelo enunciado.

## Pipeline — passo a passo

1. **Dados em HDFS.** `LI-Small_Trans.csv` (treino) e `LI-Medium_Trans.csv`
   (simulação de stream) em `/aulas/fabricio_moreira/project/dataset/`.

2. **Tabela de dimensão (multi-fonte).** `aml_stats.ipynb` constrói
   `dim_accounts` pela união dos ficheiros `LI-Small_accounts.csv` e
   `LI-Medium_accounts.csv`, deduplicada por `(bank_id, account_number)` e com
   `entity_type` derivado por regex sobre `entity_name`. Persiste em
   `/dim/accounts/` como parquet. É a segunda fonte que se cruza com a
   tabela de factos das transações.

3. **Treino offline batch (Spark MLlib).** `aml_trainer.ipynb` lê o Small, faz
   feature engineering (log dos montantes, diferença, flags `same_bank` /
   `ccy_mismatch`, hora) e treina um `RandomForest` num `Pipeline` com indexers
   + one-hot + assembler, usando **pesos de classe** para o forte
   desbalanceamento (laundering &lt;1%). Avalia AUC, F1 e matriz de confusão.

4. **Modelo persistido em HDFS.** O `PipelineModel` é gravado em
   `/model/rf_aml_pipeline/`, disponível para qualquer consumer carregar com
   `PipelineModel.load(...)`.

5. **Producer envia transações em streaming simulado.** Dois producers em
   paralelo:
   - `aml_kafka_producer.ipynb`: serializa cada linha em JSON e envia em
     batches para o tópico Kafka `transactions`, com `sleep` entre batches.
   - `aml_file_producer.ipynb`: escreve CSVs em `/stream/input/`, também em
     batches com delay.

6. **Camada de transporte:** Kafka (broker) **ou** pasta HDFS (file source) —
   duas alternativas com o mesmo processamento, para a análise comparativa.

7. **Consumer recebe + classifica num único job.** `aml_kafka_consumer.ipynb` e
   `aml_file_consumer.ipynb` são jobs Spark Structured Streaming que lêem da
   fonte, replicam a feature engineering do trainer e chamam
   `model.transform(df)`. **O modelo está embutido na pipeline**, não é um
   serviço externo — `transform` devolve um DataFrame já com `prediction` e
   `prob_fraud`.

8. **Consumer mostra no console** com coluna `status`:
   `"BLOQUEADA - suspeita de fraude"` ou `"OK - fidedigna"`, junto com
   `From Bank`, `To Bank`, `From Account`, `Amount Paid` e `prob_fraud`.

9. **Consumer grava resultados em Parquet** particionado por hora de ingestão
   em `/stream/predictions_kafka/` (e `/stream/predictions_file/` para o
   caminho file-based). Em paralelo escreve janelas de 30s com contagens em
   `/stream/metrics_*/` para o benchmark.

10. **Processor batch — KPIs e SQL ad-hoc (últimos 7 dias).** `aml_stats.ipynb`
    lê as previsões de ambas as fontes (union), **filtra à janela dos últimos
    7 dias** relativa a `MAX(Timestamp)` nas previsões, junta com `dim_accounts`
    e produz: total de transações, fidedignas vs suspeitas (n e %), valor total
    e valor suspeito, fraude por banco real, fraude por tipo de entidade (PF
    vs PJ), top contas suspeitas. Por fim regista **tabelas externas Hive**
    sobre os parquets para queries SQL puras — fecha o requisito "Spark,
    MapReduce, Hive, ..." do enunciado.

11. **Benchmark comparativo.** `aml_benchmark.ipynb` compara Kafka vs
    file-based em throughput (msgs/s) e qualidade
    (accuracy / precision / recall / F1), e o tempo batch de leitura
    Small vs Medium.

## Notebooks — taxonomia

| Categoria         | Producer                       | Consumer                        |
| ----------------- | ------------------------------ | ------------------------------- |
| Kafka             | `aml_kafka_producer.ipynb`     | `aml_kafka_consumer.ipynb`      |
| File-based (HDFS) | `aml_file_producer.ipynb`      | `aml_file_consumer.ipynb`       |
| Batch             | `aml_trainer.ipynb` (offline)  | `aml_stats.ipynb`, `aml_benchmark.ipynb` |

## Mapeamento à proposta inicial

| Passo proposto                                            | Implementação                                   | Notas                                     |
| --------------------------------------------------------- | ----------------------------------------------- | ----------------------------------------- |
| 1. Dados em HDFS                                          | HDFS                                            | ✓                                         |
| 2. Treino do modelo no Small                              | `aml_trainer.ipynb`                             | ✓                                         |
| 3. Modelo disponível                                      | `model/` em HDFS                                | ✓                                         |
| 4. Producer envia transações                              | `aml_kafka_producer` + `aml_file_producer`      | ✓ em batches, não row-by-row              |
| 5. Camada de transporte                                   | Kafka **e** pasta HDFS (ambos)                  | dois caminhos paralelos                   |
| 6 + 7. Consumer recebe e classifica                       | `aml_*_consumer.ipynb`                          | fundidos num único Spark job              |
| 8. Print de "fidedigna / suspeita"                        | sink consola com coluna `status`                | ✓                                         |
| 9. Resultados em ficheiro                                 | `predictions_*/` em parquet                     | ✓ não CSV (parquet é colunar / Hive-able) |
| 10. Estatísticas a partir dos resultados                  | `aml_stats.ipynb`                               | ✓ + `dim_accounts` + tabelas Hive            |

## Como correr (ordem para a defesa)

1. **`aml_trainer.ipynb`** — uma vez, treino offline.
2. Janelas separadas (deixar consumer arrancar primeiro):
   - `aml_kafka_consumer.ipynb` → depois `aml_kafka_producer.ipynb`
   - `aml_file_consumer.ipynb`  → depois `aml_file_producer.ipynb`
3. **`aml_stats.ipynb`** — KPIs de negócio + tabelas Hive.
4. **`aml_benchmark.ipynb`** — tabela comparativa de desempenho.

## Configuração

Parâmetros definidos como variáveis no topo de cada notebook:

- `HDFS_BASE` — namenode + diretório do projeto.
- `KAFKA_BOOTSTRAP` e `KAFKA_TOPIC` — nos dois notebooks Kafka.

O conector `spark-sql-kafka-0-10` é assumido como disponível no classpath do
cluster Spark (instalação típica em `$SPARK_HOME/jars/`).
