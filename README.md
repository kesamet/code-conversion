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
