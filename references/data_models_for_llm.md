# Модели данных для больших языковых моделей (LLM)

## Краткая справка

---

## 1. Введение

Большие языковые модели (Large Language Models, LLM) — нейросетевые модели на основе архитектуры Transformer, обученные на масштабных текстовых корпусах. Работа с LLM порождает специфические требования к организации, хранению и обработке данных, которые существенно отличаются от традиционного реляционного моделирования.

---

## 2. Жизненный цикл данных LLM

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Сбор данных │───▶│ Подготовка и │───▶│  Обучение /  │───▶│  Инференс и  │
│  (Corpora)   │    │  очистка     │    │  Fine-tuning │    │  обслуживание│
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
       │                   │                   │                    │
   Crawl, API,        Токенизация,        Модель данных       Векторные БД,
   датасеты           дедупликация,       обучения            кэширование,
                      фильтрация                              логирование
```

---

## 3. Основные модели данных

### 3.1. Данные для предобучения (Pre-training Data)

**Структура корпуса:**

| Поле | Тип | Описание |
|------|-----|----------|
| `document_id` | UUID | Уникальный идентификатор документа |
| `source` | STRING | Источник (web, books, code, wiki) |
| `language` | STRING | Язык документа (ISO 639-1) |
| `text` | TEXT | Полный текст документа |
| `token_count` | INT | Количество токенов |
| `quality_score` | FLOAT | Оценка качества (0.0–1.0) |
| `created_at` | TIMESTAMP | Дата создания/сбора |
| `metadata` | JSONB | Дополнительные атрибуты |

**Масштаб:** от сотен гигабайт до десятков терабайт текста.

**Типичные источники:**
- Common Crawl (веб-страницы)
- Wikipedia, книги, научные статьи
- GitHub (исходный код)
- Специализированные датасеты (OpenWebText, The Pile, RedPajama)

---

### 3.2. Данные для дообучения (Fine-tuning Data)

#### Supervised Fine-Tuning (SFT)

```json
{
  "id": "sft_001",
  "messages": [
    {"role": "system", "content": "Ты — эксперт по горнодобыче."},
    {"role": "user", "content": "Какие датчики устанавливают на ПДМ?"},
    {"role": "assistant", "content": "На погрузочно-доставочные машины..."}
  ],
  "metadata": {
    "domain": "mining",
    "quality": "verified",
    "source": "expert_annotation"
  }
}
```

#### RLHF (Reinforcement Learning from Human Feedback)

```json
{
  "id": "rlhf_001",
  "prompt": "Опиши причины простоя оборудования в шахте",
  "chosen": "Основные причины простоя: 1) плановое ТО...",
  "rejected": "Оборудование ломается из-за плохого качества...",
  "annotator_id": "expert_42",
  "confidence": 0.95
}
```

#### DPO (Direct Preference Optimization)

Аналогичная структура пар (chosen/rejected), но без отдельной модели вознаграждения — предпочтения напрямую оптимизируются в модели.

---

### 3.3. Векторные данные (Embeddings)

Векторные представления — ключевой элемент для RAG-систем и семантического поиска.

**Схема хранения:**

| Поле | Тип | Описание |
|------|-----|----------|
| `chunk_id` | UUID | Идентификатор фрагмента |
| `document_id` | UUID | Ссылка на исходный документ |
| `text` | TEXT | Текст фрагмента (chunk) |
| `embedding` | VECTOR(1536) | Векторное представление |
| `metadata` | JSONB | Фильтруемые атрибуты |
| `chunk_index` | INT | Порядковый номер в документе |

**Стратегии разбиения текста на фрагменты (chunking):**

```
┌─────────────────────────────────────────────┐
│            Исходный документ                │
├─────────────────────────────────────────────┤
│                                             │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐    │
│  │ Chunk 1 │  │ Chunk 2 │  │ Chunk 3 │    │  Фиксированный размер
│  └─────────┘  └─────────┘  └─────────┘    │  (напр. 512 токенов)
│                                             │
│  ┌───────────────┐  ┌──────────────────┐   │
│  │  Параграф 1   │  │   Параграф 2     │   │  Семантическое
│  └───────────────┘  └──────────────────┘   │  разбиение
│                                             │
│  ┌──────────────────────────────────────┐   │
│  │  Chunk с перекрытием (overlap)       │   │  Скользящее окно
│  └──────────────────────────────────────┘   │
└─────────────────────────────────────────────┘
```

**Популярные векторные базы данных:**

| БД | Тип | Особенности |
|----|-----|-------------|
| Pinecone | Managed SaaS | Полностью управляемый, масштабируемый |
| Weaviate | Open Source | Гибридный поиск (вектор + ключевые слова) |
| Qdrant | Open Source | Rust, высокая производительность |
| Milvus | Open Source | Горизонтальное масштабирование |
| Chroma | Open Source | Легковесный, для прототипов |
| pgvector | Расширение PG | Интеграция с PostgreSQL |
| YandexGPT Embeddings | Managed | Интеграция с Yandex Cloud |

---

### 3.4. Модель данных RAG-системы (Retrieval-Augmented Generation)

RAG — архитектурный паттерн, дополняющий LLM внешними знаниями.

```
                    ┌─────────────────┐
                    │   Пользователь  │
                    └────────┬────────┘
                             │ Запрос
                    ┌────────▼────────┐
                    │   Эмбеддинг     │
                    │   запроса       │
                    └────────┬────────┘
                             │ Вектор
              ┌──────────────▼──────────────┐
              │     Векторная БД            │
              │  (семантический поиск)      │
              └──────────────┬──────────────┘
                             │ Top-K документов
                    ┌────────▼────────┐
                    │   Формирование  │
                    │   промпта       │
                    │   (контекст +   │
                    │    запрос)      │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │      LLM       │
                    │   (генерация)   │
                    └────────┬────────┘
                             │ Ответ
                    ┌────────▼────────┐
                    │   Пользователь  │
                    └─────────────────┘
```

**Модель данных RAG-пайплайна:**

```sql
-- Источники знаний
CREATE TABLE knowledge_sources (
    source_id    UUID PRIMARY KEY,
    source_type  VARCHAR(50),    -- 'manual', 'api', 'database'
    name         VARCHAR(255),
    config       JSONB,
    last_synced  TIMESTAMP
);

-- Документы
CREATE TABLE documents (
    document_id  UUID PRIMARY KEY,
    source_id    UUID REFERENCES knowledge_sources,
    title        VARCHAR(500),
    content      TEXT,
    doc_metadata JSONB,
    created_at   TIMESTAMP,
    updated_at   TIMESTAMP
);

-- Фрагменты с эмбеддингами
CREATE TABLE chunks (
    chunk_id     UUID PRIMARY KEY,
    document_id  UUID REFERENCES documents,
    chunk_index  INT,
    text         TEXT,
    embedding    VECTOR(1536),
    token_count  INT,
    metadata     JSONB
);

-- Индекс для семантического поиска
CREATE INDEX idx_chunks_embedding
ON chunks USING ivfflat (embedding vector_cosine_ops);
```

---

### 3.5. Графы знаний (Knowledge Graphs) для LLM

Графы знаний дополняют векторный поиск структурированными связями между сущностями.

**Модель GraphRAG:**

```
(Оборудование:ПДМ-7)──[УСТАНОВЛЕНО_В]──▶(Шахта:Северная)
        │                                        │
   [ИМЕЕТ_ДАТЧИК]                          [СОДЕРЖИТ]
        │                                        │
        ▼                                        ▼
(Датчик:Вибрация_01)                    (Горизонт:H-320)
        │
   [ГЕНЕРИРУЕТ]
        │
        ▼
(Показание: {value: 4.2, ts: ...})
```

**Применение в LLM:**
- **GraphRAG** — извлечение связанных сущностей для обогащения контекста
- **Верификация фактов** — проверка утверждений модели по графу
- **Навигация по знаниям** — многошаговые рассуждения через цепочки связей

---

## 4. Модели данных для операционной работы с LLM

### 4.1. Логирование запросов и ответов

```sql
CREATE TABLE llm_interactions (
    interaction_id  UUID PRIMARY KEY,
    user_id         UUID,
    session_id      UUID,
    model           VARCHAR(100),     -- 'gpt-4', 'yandexgpt', 'llama-3'
    prompt_tokens   INT,
    completion_tokens INT,
    total_cost      DECIMAL(10,6),
    latency_ms      INT,
    prompt          TEXT,
    completion      TEXT,
    temperature     FLOAT,
    created_at      TIMESTAMP
);
```

### 4.2. Оценка качества ответов

```sql
CREATE TABLE response_evaluations (
    eval_id         UUID PRIMARY KEY,
    interaction_id  UUID REFERENCES llm_interactions,
    evaluator_type  VARCHAR(50),      -- 'human', 'auto', 'llm-as-judge'
    relevance       FLOAT,            -- 0.0–1.0
    faithfulness    FLOAT,            -- соответствие контексту
    harmfulness     FLOAT,            -- токсичность
    feedback_text   TEXT,
    created_at      TIMESTAMP
);
```

### 4.3. Управление промптами (Prompt Management)

```sql
CREATE TABLE prompt_templates (
    template_id     UUID PRIMARY KEY,
    name            VARCHAR(255),
    version         INT,
    system_prompt   TEXT,
    user_template   TEXT,             -- с плейсхолдерами {variable}
    variables       JSONB,            -- описание переменных
    model_config    JSONB,            -- temperature, max_tokens и др.
    is_active       BOOLEAN,
    created_at      TIMESTAMP,
    updated_by      VARCHAR(100)
);
```

---

## 5. Специфика данных для LLM в промышленности

### Применение к предприятию «Руда+»

| Задача | Модель данных | Технология |
|--------|--------------|------------|
| Поиск по технической документации | RAG + векторная БД | pgvector / Qdrant |
| Анализ инцидентов | Граф знаний + LLM | Neo4j + LangChain |
| Предиктивное обслуживание | Временные ряды → промпт | TimescaleDB + YandexGPT |
| Ответы оператору в реальном времени | RAG + streaming | Kafka + LLM API |
| Классификация причин простоя | Fine-tuned модель | SFT-датасет + модель |

### Пример: RAG для технической документации

```python
# Пример индексации документации в векторную БД
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.embeddings import YandexGPTEmbeddings
from langchain.vectorstores import PGVector

# 1. Разбиение документации на фрагменты
splitter = RecursiveCharacterTextSplitter(
    chunk_size=512,
    chunk_overlap=50,
    separators=["\n\n", "\n", ". ", " "]
)
chunks = splitter.split_documents(technical_docs)

# 2. Создание эмбеддингов и загрузка в БД
embeddings = YandexGPTEmbeddings(
    api_key="...",
    model_uri="emb://..."
)
vectorstore = PGVector.from_documents(
    documents=chunks,
    embedding=embeddings,
    connection_string="postgresql://user:pass@host:5432/ruda_rag"
)

# 3. Семантический поиск
results = vectorstore.similarity_search(
    "Какой интервал ТО для погрузочно-доставочной машины?",
    k=5
)
```

---

## 6. Сравнение подходов к хранению данных для LLM

| Критерий | Реляционная БД | Векторная БД | Граф знаний | Объектное хранилище |
|----------|---------------|-------------|-------------|-------------------|
| **Тип данных** | Структурированные | Эмбеддинги | Связи между сущностями | Сырые файлы |
| **Поиск** | SQL-запросы | Семантическая близость | Обход графа | По метаданным |
| **Масштаб** | До ТБ | До млрд векторов | До млрд узлов | Без ограничений |
| **Применение** | Логи, метрики, промпты | RAG, поиск | Верификация, навигация | Обучающие корпуса |
| **Пример** | PostgreSQL | Qdrant, pgvector | Neo4j | S3, Yandex Object Storage |

---

## 7. Ключевые метрики качества данных для LLM

| Метрика | Описание | Целевое значение |
|---------|----------|-----------------|
| **Дедупликация** | Доля уникальных документов | > 95% |
| **Токсичность** | Доля вредоносного контента | < 0.1% |
| **Языковой баланс** | Соотношение языков в корпусе | Зависит от задачи |
| **Актуальность** | Средний возраст документов | < 6 мес. для RAG |
| **Покрытие домена** | Полнота по предметной области | > 80% тем |
| **Faithfulness (RAG)** | Соответствие ответа контексту | > 0.85 |
| **Relevance (RAG)** | Релевантность найденных документов | > 0.80 |

---

## 8. Архитектура данных LLM-приложения

```
┌─────────────────────────────────────────────────────────────────┐
│                    LLM Application Layer                        │
├─────────────┬──────────────┬──────────────┬────────────────────┤
│  Prompt     │   RAG        │   Agent      │   Evaluation       │
│  Management │   Pipeline   │   Framework  │   & Monitoring     │
├─────────────┴──────────────┴──────────────┴────────────────────┤
│                    Orchestration (LangChain / LlamaIndex)       │
├────────────────────────────────────────────────────────────────┤
│                         Data Layer                              │
├──────────┬──────────┬──────────┬──────────┬───────────────────┤
│ Relational│ Vector  │  Graph   │  Object  │  Message Queue    │
│ (PG)      │ (Qdrant)│ (Neo4j)  │ (S3)     │  (Kafka)          │
│           │         │          │          │                    │
│ • Логи    │ • Chunks│ • Связи  │ • Корпуса│ • Стриминг        │
│ • Промпты │ • Embeds│ • Факты  │ • Модели │ • События         │
│ • Метрики │ • Поиск │ • Онтол. │ • Файлы  │ • Обновления      │
└──────────┴──────────┴──────────┴──────────┴───────────────────┘
```

---

## 9. Рекомендуемые инструменты и платформы

| Категория | Инструмент | Назначение |
|-----------|-----------|------------|
| Оркестрация | LangChain, LlamaIndex | Построение RAG и агентов |
| Векторные БД | pgvector, Qdrant, Milvus | Хранение и поиск эмбеддингов |
| Мониторинг | LangSmith, Langfuse | Трассировка и оценка качества |
| Данные | DVC, LakeFS | Версионирование данных |
| Fine-tuning | Hugging Face, Axolotl | Дообучение моделей |
| LLM API | YandexGPT, GigaChat | Российские LLM-сервисы |

---

## 10. Источники и дополнительные материалы

1. **RAG:** Lewis et al., "Retrieval-Augmented Generation for Knowledge-Intensive NLP Tasks" (2020)
2. **GraphRAG:** Microsoft Research, "From Local to Global: A Graph RAG Approach" (2024)
3. **Vector Databases:** Обзор: pinecone.io/learn/vector-database
4. **LangChain Documentation:** docs.langchain.com
5. **pgvector:** github.com/pgvector/pgvector
6. **YandexGPT API:** yandex.cloud/docs/foundation-models

---

*Справка подготовлена в рамках курса «Практикум по анализу и моделированию данных»*
*Дата: февраль 2026*
