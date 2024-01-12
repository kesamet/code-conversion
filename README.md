# code-conversion
Code conversion from SAS using Google Gemini / CodeLlama


## 🔧 Getting Started

You will need to set up your development environment using conda, which you can install [directly](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html).

```bash
conda create --name codeconvert python=3.11
conda activate codeconvert
pip install -r requirements.txt
```

`GOOGLE_API_KEY` is required in order to use Gemini. Add it in `.env`.


## 🔍 Usage

```bash
python -m code_convert -i sample.sas -o sample_translated.py
```


## Notes

- The connection and disconnection steps are omitted from the SQL code as they are specific to SAS and depend on the way the database connection is established and managed in your SAS environment. In Spark SQL, database connections are typically managed outside of the SQL code.
- In Spark SQL, we create a temporary table to mimic the behavior of a volatile table in SAS. Temporary tables are specific to the session/connection and are automatically dropped at the end of the session.
- The translation assumes you have the necessary data loaded into Spark's DataFrame or table format before running these Spark SQL statements.
- The data types might need to be adjusted according to the actual data types used in your database for each column.
- The output code might need further customization based on the specific DBMS you are using, the database schema, and other considerations.
