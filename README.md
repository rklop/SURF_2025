# Text-to-SQL Model

A comprehensive text-to-SQL generation and verification system that converts natural language questions to SQL queries and verifies their equivalence using formal verification methods.

## Overview

This project implements a two-stage approach to text-to-SQL generation:
1. **TASL (Table-Aware Schema Learning)**: Analyzes database schemas and generates column meaning descriptions
2. **TALOG (Table-Aware Logic Generation)**: Converts natural language questions to SQL queries using LLM-based reasoning

The system also includes SQL equivalence verification using the veriEQL framework to ensure generated SQL queries are semantically equivalent to ground truth queries.

## Project Structure

```
TA-SQL/
├── src/                    # Core modules
│   ├── modules.py         # TASL and TALOG implementations
│   ├── llm.py            # LLM integration (Gemini API)
│   └── prompt_bank.py    # Prompt templates
├── evaluation/            # Evaluation scripts
├── data/                  # Development datasets
├── dev_data/             # Development data and constraints
├── outputs/              # Generated results and outputs
├── verieql/              # SQL verification framework
├── main.py               # Main execution script
├── run.py                # SQL generation pipeline
└── requirements.txt      # Python dependencies
```

## Features

- **Schema-Aware Generation**: Leverages database schema information for accurate SQL generation
- **Multi-Modal Input**: Supports natural language questions with optional evidence
- **Formal Verification**: Uses veriEQL to verify SQL equivalence with configurable bound sizes
- **Multiple Database Support**: Works with various database schemas (SQLite, MySQL, PostgreSQL)
- **Comprehensive Evaluation**: Includes execution-based and verification-based evaluation metrics

## Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd "text2sql model"
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
cd TA-SQL
pip install -r requirements.txt
```

4. Set up API keys:
```bash
export GEMINI_API_KEY="your_gemini_api_key_here"
```

## Usage

### Basic SQL Generation

Generate SQL queries from natural language questions:

```bash
python3 run.py \
  --db_root_path "./data/dev_databases" \
  --column_meaning_path "./outputs/column_meaning.json" \
  --mode "dev" \
  --output_path "./outputs/predict_dev.json"
```

### Complete Pipeline

Run the complete pipeline including column meaning generation:

```bash
# Generate column descriptions (optional)
python3 ./src/conclude_meaning.py \
  --db_root_path "./data/dev_databases" \
  --mode "dev" \
  --output_path "./outputs/column_meaning.json"

# Generate SQL queries
python3 ./run.py \
  --db_root_path "./data/dev_databases" \
  --mode "dev" \
  --column_meaning_path "./outputs/column_meaning.json" \
  --output_path "./outputs/predict_dev.json"
```

### SQL Verification

Verify SQL equivalence using the main script:

```bash
python3 main.py \
  --db_root_path "./temp_data/temp_databases" \
  --column_meaning_path "./outputs/temp_meaning.json" \
  --mode "temp" \
  --output_path "./outputs/temp_test.json"
```

### Evaluation

Run evaluation scripts to assess model performance:

```bash
# Execution-based evaluation
python3 ./evaluation/evaluation_ex.py \
  --predicted_sql_path "./dev_data/dev.json" \
  --ground_truth_path "./dev_data/dev.sql" \
  --db_root_path "./dev_data/dev_databases/" \
  --sql_dialect "SQLite"

# Verification-based evaluation
python3 ./evaluation/evaluation_ves.py \
  --predicted_sql_path "./outputs/" \
  --ground_truth_path "./dev_data/dev_databases/" \
  --data_mode "dev"
```

## Configuration

### Command Line Arguments

- `--db_root_path`: Path to database files
- `--column_meaning_path`: Path to column meaning descriptions
- `--mode`: Dataset mode (dev, test, temp)
- `--output_path`: Output file path for generated SQL

### Environment Variables

- `GEMINI_API_KEY`: Required for LLM integration

## Data Format

### Input Questions
```json
{
  "question": "What is the average score for students in grade 5?",
  "evidence": "Students table contains grade and score columns",
  "db_id": "california_schools"
}
```

### Database Schema
```json
{
  "db_id": "california_schools",
  "table_names": ["schools", "students", "scores"],
  "column_names": [["schools", "id"], ["students", "grade"], ["scores", "value"]]
}
```

## Architecture

### TASL Module
- Analyzes database schemas
- Generates column meaning descriptions
- Filters relevant tables and columns

### TALOG Module
- Generates schema-aware prompts
- Converts questions to SQL using LLM reasoning
- Handles foreign key relationships

### Verification
- Uses veriEQL framework for SQL equivalence checking
- Configurable bound sizes for verification
- Generates counterexamples for non-equivalent queries

## Evaluation Metrics

- **Execution Accuracy**: Measures if generated SQL produces correct results
- **Verification Success**: Checks SQL equivalence using formal methods
- **Performance Metrics**: Execution time and verification time

## Supported Databases

- SQLite
- MySQL
- PostgreSQL
- Custom database schemas

## Dependencies

Key dependencies include:
- `google-generativeai`: Gemini API integration
- `pandas`: Data manipulation
- `sqlite3`: Database connectivity
- `verieql`: SQL verification framework
- `z3-solver`: SMT solver for verification

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

[Add license information here]

## Citation

If you use this work in your research, please cite:

```
[Add citation information here]
```

## Contact

For questions and support, please contact [contact information].
