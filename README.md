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

1. **`aml_trainer.ipynb`** — uma vez, treino offline.
2. Janelas separadas (deixar consumer arrancar primeiro):
   - `aml_kafka_consumer.ipynb` → depois `aml_kafka_producer.ipynb`
3. **`aml_stats.ipynb`** — KPIs de negócio + tabelas Hive.
4. **`aml_benchmark.ipynb`** — qualidade + throughput + Small vs Medium.

## Configuração

Parâmetros definidos como variáveis no topo de cada notebook:

- `HDFS_BASE` — namenode + diretório do projeto.
- `KAFKA_BOOTSTRAP` e `KAFKA_TOPIC` — nos dois notebooks Kafka.

O conector `spark-sql-kafka-0-10` é assumido como disponível no classpath do
cluster Spark (instalação típica em `$SPARK_HOME/jars/`).
