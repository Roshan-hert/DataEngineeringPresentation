## Setup and Execution

### 1. Clone the Repository

Clone the project from the `main` branch:

```bash
git clone -b main https://github.com/Roshan-hert/DataEngineeringPresentation.git
```

Navigate to the project directory:

```bash
cd DataEngineeringPresentation
```

### 2. Install Dependencies

Ensure Python is installed, then run:

```bash
%pip install -r ../requirements.txt
```

### 3. Start Jupyter Lab

From the project root directory:

```bash
jupyter lab
```

### 4. Run the ETL Pipeline

Open `notebook/ETL.ipynb` and execute the cells in order.

The pipeline extracts CRM data, performs data cleaning and validation, generates customer-level features, and exports the processed datasets.

### 5. Output

The CRM pipeline produces:

- `output/cleaned_crm.parquet` — Cleaned CRM interaction records.
- `output/crm_features.parquet` — Customer-level interaction and complaint features.
