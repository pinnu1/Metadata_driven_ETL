# Metadata-Driven ETL Framework

A flexible, metadata-driven ETL (Extract, Transform, Load) framework built on Apache Spark and Databricks Delta Live Tables (DLT). This framework enables dynamic data pipeline configuration through JSON metadata, eliminating the need for hardcoded transformations and enabling rapid pipeline development.

## 🎯 Purpose

This framework implements a medallion architecture (Bronze → Silver → Gold) that:
- **Extracts** data from Azure SQL Server databases
- **Transforms** data through configurable, reusable transformation steps
- **Loads** data into analytical layers for business intelligence

Instead of writing custom code for each data pipeline, users simply define the pipeline behavior in JSON metadata files, making it easy to maintain and scale data operations.

## 🏗️ Architecture

The framework follows the **Medallion Architecture** pattern with three distinct layers:

```
┌─────────────────┐
│  Azure SQL DB   │ (Source)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  BRONZE LAYER   │ (Raw Data Ingestion)
│  - Streaming    │
│  - Incremental  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SILVER LAYER   │ (Cleaned & Enriched)
│  - Deduplication│
│  - PII Masking  │
│  - Joins        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   GOLD LAYER    │ (Business KPIs)
│  - Aggregations │
│  - Analytics    │
└─────────────────┘
```

### Bronze Layer
- **Purpose**: Raw data ingestion with minimal transformation
- **Features**: 
  - JDBC streaming from Azure SQL Server
  - Full and incremental load support (watermark-based)
  - Schema preservation from source

### Silver Layer
- **Purpose**: Data cleaning, enrichment, and standardization
- **Features**:
  - Step-based transformations (configurable via metadata)
  - PII masking (SHA256 hashing)
  - Deduplication and data quality checks
  - Dimension table joins
  - Surrogate key generation
  - Null handling and data type casting

### Gold Layer
- **Purpose**: Business-ready analytical tables and KPIs
- **Features**:
  - SQL-based transformations
  - Template-based KPI generation
  - Pre-built aggregation templates

## 📁 Directory Structure

```
Metadata_driven_ETL/
│
├── engine/                      # Core ETL processing engines
│   ├── metadata_loader.py      # Loads metadata from JSON/URL
│   ├── bronze_engine.py        # Bronze layer ingestion logic
│   ├── silver_engine.py        # Silver layer transformation logic
│   ├── gold_engine.py          # Gold layer KPI generation logic
│   └── gold_kpi_templates.py   # Predefined KPI templates
│
├── Metadata/                    # JSON metadata configuration files
│   └── metadata1.json          # Sample metadata configuration
│
├── Pipelines/                   # Pipeline orchestration
│   └── dlt_pipeline.py         # Main DLT pipeline execution script
│
├── utils/                       # Reusable utility functions
│   └── silver_common_utils.py  # Silver layer transformation functions
│
└── testing/                     # Jupyter notebooks for testing
```

## 🛠️ Technology Stack

- **Language**: Python 3.x
- **Data Processing**: Apache Spark (PySpark)
- **Pipeline Framework**: Databricks Delta Live Tables (DLT)
- **Source Database**: Azure SQL Server
- **Data Format**: Delta Lake
- **Metadata Storage**: JSON (GitHub-hosted)

## 📋 Prerequisites

Before running this ETL framework, ensure you have:

1. **Databricks Workspace** with Delta Live Tables enabled
2. **Azure SQL Server** access with:
   - Server hostname
   - Database name
   - Username and password
   - Network connectivity from Databricks
3. **Python 3.x** with PySpark
4. **Access to metadata JSON** file (GitHub or local)

## ⚙️ Metadata Configuration

The framework is driven by a JSON metadata file that defines the entire ETL pipeline. Here's the structure:

```json
{
  "pipeline": {
    "name": "metadata_driven_dlt_pipeline",
    "load_strategy": "load_once"
  },
  
  "azure_sql": {
    "server": "your-server.database.windows.net",
    "username": "your-username",
    "password": "your-password"
  },
  
  "tables": [
    {
      "table_name": "customer",
      "enabled": true,
      
      "layers": {
        "bronze": {
          "batch_precedence_id": 1,
          "type": "streaming",
          "source": {
            "system": "azure_sql",
            "database": "your_database",
            "schema": "dbo",
            "table": "Customer"
          },
          "incremental": {
            "enabled": true,
            "column": "ModifiedDate"
          },
          "target": {
            "table": "bronze_customer"
          }
        },
        
        "silver": {
          "batch_precedence_id": 2,
          "dependency_type": "base",
          "input": "bronze_customer",
          "steps": [
            {"step_type": "generic_cleaning"},
            {"step_type": "lowercase_columns"},
            {"step_type": "mask_pii", "columns": ["email", "phone"]}
          ],
          "output": "silver_customer"
        },
        
        "gold": {
          "batch_precedence_id": 3,
          "kpi_type": "template",
          "template_name": "customer_sales_template",
          "output": "gold_customer_kpi"
        }
      }
    }
  ]
}
```

### Key Metadata Elements:

- **pipeline**: Pipeline-level configuration (name, load strategy)
- **azure_sql**: Source database connection details
- **tables**: Array of table configurations, each containing:
  - **enabled**: Boolean to enable/disable the table
  - **layers**: Configuration for bronze, silver, and gold layers
  - **batch_precedence_id**: Execution order (lower runs first)

### Available Silver Transformation Steps:

- `generic_cleaning`: Deduplication, date casting, audit timestamps
- `lowercase_columns`: Convert all column names to lowercase
- `mask_pii`: SHA256 hash sensitive columns
- `add_surrogate_key`: Generate deterministic surrogate keys
- `handle_nulls`: Null value handling
- `filter_rows`: Apply row-level filters
- `drop_columns`: Remove specified columns
- `add_business_rules`: Apply custom business logic

## 🚀 How to Run

### Step 1: Prepare Metadata

1. Create or modify a metadata JSON file (see `Metadata/metadata1.json` as example)
2. Upload it to GitHub or store it locally
3. Update the metadata URL in `Pipelines/dlt_pipeline.py`:

```python
METADATA_PATH = "https://raw.githubusercontent.com/your-repo/your-branch/metadata.json"
```

### Step 2: Configure Credentials

Update database credentials in `Pipelines/dlt_pipeline.py`:

```python
metadata["azure_sql"]["password"] = "your-password"
metadata["azure_sql"]["username"] = "your-username"
```

**Note**: For production, use Databricks Secrets instead of hardcoding credentials.

### Step 3: Deploy to Databricks

1. Upload the entire repository to Databricks workspace or Git integration
2. Create a new Delta Live Tables pipeline in Databricks
3. Set the pipeline source to `Pipelines/dlt_pipeline.py`
4. Configure pipeline settings:
   - Target database/schema
   - Cluster configuration
   - Pipeline mode (triggered or continuous)

### Step 4: Run the Pipeline

1. Start the DLT pipeline from Databricks UI
2. Monitor execution in the DLT graph view
3. Check for errors in the pipeline logs

### Step 5: Verify Results

Query the resulting Delta tables:

```sql
-- Bronze layer (raw data)
SELECT * FROM bronze_customer LIMIT 10;

-- Silver layer (cleaned data)
SELECT * FROM silver_customer LIMIT 10;

-- Gold layer (KPIs)
SELECT * FROM gold_customer_kpi LIMIT 10;
```

## 📊 Workflow Execution

The pipeline executes in the following order:

1. **Load Metadata**: Fetch metadata from GitHub URL
2. **Override Credentials**: Inject secure credentials
3. **Bronze Layer**: Process all enabled bronze tables (parallel execution possible)
4. **Silver Base Tables**: Process base transformations sequentially by batch_precedence_id
5. **Silver Join Tables**: Process dimension joins after base tables complete
6. **Gold Layer**: Generate KPI tables from silver data

## 🔒 Security Best Practices

- **Never commit credentials** to metadata files in version control
- Use **Databricks Secrets** or environment variables for sensitive data
- Implement **PII masking** in silver layer using the `mask_pii` transformation
- Enable **audit logging** through generic_cleaning step (adds _audit_timestamp)

## 🔧 Customization

### Adding Custom Transformations

Add new transformation functions in `utils/silver_common_utils.py`:

```python
def my_custom_transformation(df, params):
    # Your transformation logic
    return df
```

Register in `engine/silver_engine.py`:

```python
elif step["step_type"] == "my_custom_transformation":
    df = my_custom_transformation(df, step.get("params", {}))
```

### Adding Custom KPI Templates

Add new templates in `engine/gold_kpi_templates.py`:

```python
def my_kpi_template(metadata):
    return """
        SELECT 
            category,
            COUNT(*) as total_count,
            SUM(amount) as total_amount
        FROM {input_table}
        GROUP BY category
    """
```

## 🐛 Troubleshooting

### Common Issues:

1. **JDBC Connection Failures**
   - Verify Azure SQL Server firewall allows Databricks IP ranges
   - Check credentials are correct
   - Ensure database/schema/table names match exactly

2. **Metadata Loading Errors**
   - Verify JSON syntax is valid
   - Check GitHub URL is accessible from Databricks
   - Ensure metadata structure matches expected format

3. **Transformation Failures**
   - Check column names match in metadata
   - Verify step types are spelled correctly
   - Review DLT pipeline logs for detailed error messages

## 📝 Example Use Cases

- **Customer Data Integration**: Ingest customer data from multiple sources, clean, and create 360° views
- **Sales Analytics**: Load transaction data and generate sales KPIs and dashboards
- **Data Quality Pipelines**: Implement automated data quality checks and remediation
- **Regulatory Compliance**: Mask PII data while maintaining analytical value

## 🤝 Contributing

To extend or modify this framework:

1. Fork the repository
2. Create a feature branch
3. Add your changes (engines, utils, templates)
4. Test with sample metadata
5. Submit a pull request

## 📄 License

This project is provided as-is for educational and commercial use.

## 👥 Author

Developed as a reusable, metadata-driven ETL framework for modern data platforms.

---

**Happy Data Engineering! 🚀**
