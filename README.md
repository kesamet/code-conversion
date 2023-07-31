# code-conversion
Code conversion from SAS using Google PaLM


## 🔧 Getting Started

You will need to install `google-generativeai` and `tqdm`.

```bash
pip install -r requirements.txt
```

`PALM_API_KEY` is required. Add it in `.env`.


## 🔍 Usage

```bash
python -m code_convert -i sample.sas -o sample_translated.py
```


## Notes

- Estimated cost of translating 400 lines of code: 10,000 tokens => USD 0.05
- The connection and disconnection steps are omitted from the SQL code as they are specific to SAS and depend on the way the database connection is established and managed in your SAS environment. In Spark SQL, database connections are typically managed outside of the SQL code.
- In Spark SQL, we create a temporary table to mimic the behavior of a volatile table in SAS. Temporary tables are specific to the session/connection and are automatically dropped at the end of the session.
- The translation assumes you have the necessary data loaded into Spark's DataFrame or table format before running these Spark SQL statements.
- The data types might need to be adjusted according to the actual data types used in your database for each column.
- The output code might need further customization based on the specific DBMS you are using, the database schema, and other considerations.
