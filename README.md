# 🤖 GenAI-Powered ETL Agent

An intelligent ETL (Extract-Transform-Load) pipeline orchestrated by a GenAI agent using **LangGraph** and **Azure OpenAI**.

## 🎯 Overview

This project demonstrates how Large Language Models (LLMs) can be used to create intelligent, self-correcting ETL pipelines. Instead of hardcoded transformation rules, the agent **reasons** about data, **plans** transformations dynamically, and **adapts** to changes automatically.

### Key Features

- **🧠 Intelligent Schema Analysis**: LLM understands data structure and identifies quality issues
- **📋 Dynamic Transformation Planning**: Agent generates transformation steps based on data characteristics
- **🔄 Self-Correcting Pipeline**: Automatic error recovery with LLM-guided re-planning
- **🎯 Schema Adaptation**: Handles schema changes without code modifications
- **✅ Validation Generation**: LLM creates appropriate validation rules

---

## 🏗️ Architecture

```
╔══════════════════════════════════════════════════════════════════════════╗
║                    GENAI ETL AGENT ARCHITECTURE                          ║
╠══════════════════════════════════════════════════════════════════════════╣
║                                                                          ║
║    ┌─────────────────────────────────────────────────────────────────┐   ║
║    │                    LangGraph State Machine                       │   ║
║    │                                                                  │   ║
║    │   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │   ║
║    │   │ EXTRACT  │───▶│ ANALYZE  │───▶│   PLAN   │───▶│TRANSFORM │  │   ║
║    │   └──────────┘    └──────────┘    └──────────┘    └──────────┘  │   ║
║    │        │               │               │               │        │   ║
║    │        ▼               ▼               ▼               ▼        │   ║
║    │   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │   ║
║    │   │   Tool   │    │   LLM    │    │   LLM    │    │   Tool   │  │   ║
║    │   │ extract  │    │ analyze  │    │  plan    │    │transform │  │   ║
║    │   └──────────┘    └──────────┘    └──────────┘    └──────────┘  │   ║
║    │                                                                  ║
║    │                         ┌───────────────────┐                    │   ║
║    │   ┌──────────┐         │   ERROR HANDLER   │                    │   ║
║    │   │ VALIDATE │◀────────│   (LLM Recovery)  │◀────── on error    │   ║
║    │   └──────────┘         └───────────────────┘                    │   ║
║    │        │                        │                               │   ║
║    │        ▼                        │ retry                         │   ║
║    │   ┌──────────┐    ┌──────────┐  │                               │   ║
║    │   │   LOAD   │───▶│  VERIFY  │──┴──▶ END                        │   ║
║    │   └──────────┘    └──────────┘                                  │   ║
║    │                                                                  │   ║
║    └─────────────────────────────────────────────────────────────────┘   ║
║                                                                          ║
║    ┌─────────────────────────────────────────────────────────────────┐   ║
║    │                    Azure OpenAI (LLM)                            │   ║
║    │  • Schema analysis & understanding                               │   ║
║    │  • Transformation planning                                       │   ║
║    │  • Validation rule generation                                    │   ║
║    │  • Error recovery & re-planning                                  │   ║
║    └─────────────────────────────────────────────────────────────────┘   ║
║                                                                          ║
╚══════════════════════════════════════════════════════════════════════════╝
```

---

## 🚀 How the Agent Reasons and Plans

### Step 1: Extract & Analyze

The agent extracts data and performs deep schema analysis:

```python
# Agent automatically detects:
- Column types (numeric, datetime, categorical)
- Null values and their percentages  
- Outliers and invalid values
- Potential type mismatches
```

### Step 2: LLM-Powered Planning

The LLM receives the schema analysis and generates a transformation plan:

```
INPUT (to LLM):
- Schema summary with 18 columns
- Data quality issues: negative passenger counts, invalid rate codes
- User instructions: "Clean data for analysis"

OUTPUT (from LLM):
[
    {"action": "convert_datetime", "column": "pickup_datetime"},
    {"action": "fill_null", "column": "passenger_count", "strategy": "median"},
    {"action": "remove_negative", "column": "passenger_count"},
    {"action": "remove_invalid", "column": "RatecodeID", "valid_values": [1,2,3,4,5,6]}
]
```

### Step 3: Execute & Validate

The agent executes transformations and generates validation rules:

```
Transformation Log:
✅ Step 1: convert_datetime on 'pickup_datetime' - Success
✅ Step 2: fill_null on 'passenger_count' - Filled 1 null values
✅ Step 3: remove_negative on 'passenger_count' - Removed 1 rows

Validation:
✅ passenger_count should not have nulls - Passed
✅ fare_amount should be positive - Passed
✅ Should have at least 10 rows - Passed (23 rows)
```

### Step 4: Error Recovery (Self-Healing)

If something fails, the LLM suggests a recovery plan:

```
ERROR: Column 'RatecodeID' contains non-numeric value 'INVALID'

LLM RECOVERY:
"The RatecodeID column has mixed types. Adding type conversion before validation."

NEW PLAN:
[
    {"action": "convert_numeric", "column": "RatecodeID"},
    {"action": "fill_null", "column": "RatecodeID", "strategy": "mode"}
]
```

---

## 📁 Project Structure

```
genai_etl_agent/
├── .env.example              # Environment variables template
├── requirements.txt          # Python dependencies
├── README.md                 # This file
│
├── data/
│   ├── nyc_taxi_sample.csv       # Sample NYC taxi data
│   └── nyc_taxi_new_schema.csv   # Schema-changed version (for bonus demo)
│
├── output/
│   └── (generated SQLite databases)
│
└── src/
    ├── __init__.py
    ├── llm_setup.py          # Azure OpenAI configuration
    ├── main.py               # Entry point with demos
    │
    ├── agent/
    │   ├── __init__.py
    │   └── etl_agent.py      # LangGraph agent implementation
    │
    └── tools/
        ├── __init__.py
        ├── extract_tool.py   # Data extraction & schema analysis
        ├── transform_tool.py # Transformation execution
        └── load_tool.py      # Database loading
```

---

## 🛠️ Setup & Installation

### 1. Clone and Setup

```bash
cd genai_etl_agent
pip install -r requirements.txt
```

### 2. Configure Azure OpenAI

Create a `.env` file with your credentials:

```env
OPENAI_API_KEY=your_api_key_here
OPENAI_DEPLOYMENT_ENDPOINT=https://your-resource.openai.azure.com/
OPENAI_DEPLOYMENT_NAME=your_deployment_name
OPENAI_DEPLOYMENT_VERSION=2024-02-15-preview
OPENAI_MODEL_NAME=gpt-4
```

### 3. Run the Demo

```bash
cd src
python main.py
```

---

## 🎮 Demo Scenarios

### Demo 1: Basic ETL Pipeline

Demonstrates the complete ETL flow with NYC taxi data:
- Schema analysis with quality issue detection
- LLM-generated transformation plan
- Automatic validation
- SQLite database loading

### Demo 2: Schema Change Adaptation (Bonus)

Shows how the agent handles a completely different schema:
- Original: `VendorID, tpep_pickup_datetime, passenger_count, ...`
- New: `vendor_id, pickup_time, num_passengers, ...`

The agent automatically adapts without code changes!

---

## 🔧 Trade-offs and Limitations

### Trade-offs Made

| Decision | Trade-off |
|----------|-----------|
| **SQLite** | Simple PoC vs production scalability |
| **Small dataset** | Fast iteration vs large-scale testing |
| **Predefined transforms** | Safety vs flexibility |
| **JSON transformation plans** | Structured output vs natural language |

### Current Limitations

1. **Scale**: Designed for small-scale PoC data
2. **Transform Safety**: Only predefined transformations are allowed (prevents arbitrary code execution)
3. **Error Recovery**: Limited to 2 retries
4. **LLM Latency**: Each planning step requires an LLM call

### Production Considerations

For production use, consider:
- Connection pooling for databases
- Async execution for large datasets
- Caching of common transformation plans
- More robust error handling and logging
- Data lineage tracking

---

## 🎁 Bonus: Adaptation Scenarios

### If Schema Changes

The agent handles schema changes automatically:

1. **Detection**: Schema analyzer identifies new/renamed columns
2. **Adaptation**: LLM plans transformations based on semantic understanding
3. **Mapping**: Agent can map `passenger_count` → `num_passengers`

### If Data Quality Degrades

The agent responds to quality degradation:

1. **Detection**: More quality issues flagged in analysis
2. **Planning**: LLM generates additional cleaning steps
3. **Validation**: Stricter rules generated for problematic columns

### If New Data Source Added

The agent adapts to new sources:

1. **Extraction**: Tool detects file type (CSV, JSON, Parquet)
2. **Analysis**: Schema analyzer works with any tabular data
3. **Planning**: LLM understands domain regardless of source

---

## 📊 What Makes This Stand Out

1. **True Reasoning**: The agent doesn't just execute rules—it *thinks* about data
2. **Self-Healing**: Automatic error recovery with LLM guidance
3. **Natural Language Interface**: Describe transformations in plain English
4. **Minimal Configuration**: No hardcoded rules for data types or transformations
5. **Observable**: Full reasoning log shows agent's decision process

---

## 🧪 Testing the Agent

```python
from agent.etl_agent import create_etl_agent

# Create agent
agent = create_etl_agent()

# Run pipeline with natural language instructions
result = agent.run(
    source_path="./data/nyc_taxi_sample.csv",
    target_db="./output/my_data.db",
    target_table="trips",
    user_instructions="Clean the data, handle missing values, remove outliers"
)

# Check results
print(f"Status: {result['final_status']}")
print(f"Rows loaded: {result['load_result']['rows_loaded']}")
```

---

## 📚 Technologies Used

- **LangGraph**: State machine orchestration for agent workflow
- **LangChain**: LLM integration and tooling
- **Azure OpenAI**: GPT-4 for reasoning and planning
- **Pandas**: Data manipulation
- **SQLite**: Target database

---

## 📄 License

MIT License - Feel free to use and modify for your own projects.
