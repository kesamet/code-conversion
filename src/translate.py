import logging
import os
from typing import Dict

import google.generativeai as genai
from langchain.llms.ctransformers import CTransformers
from langchain.prompts import PromptTemplate
from langchain.schema import StrOutputParser
from langchain.schema.runnable import RunnablePassthrough

logging.basicConfig(level=logging.INFO)

MODELS_DIR = "./models"

QUERY = """Translate the following SAS code, delimited by triple backticks, to Spark SQL
```sas
{code_snippet}
```"""


class Gemini:
    def __init__(self):
        genai.configure(api_key=os.environ["GOOGLE_API_KEY"])

        generation_config = {
            "temperature": 0.4,
            "top_p": 1,
            "top_k": 32,
            "max_output_tokens": 4096,
        }

        safety_settings = [
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_MEDIUM_AND_ABOVE"
            },
            {
                "category": "HARM_CATEGORY_HATE_SPEECH",
                "threshold": "BLOCK_MEDIUM_AND_ABOVE"
            },
            {
                "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
                "threshold": "BLOCK_MEDIUM_AND_ABOVE"
            },
            {
                "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
                "threshold": "BLOCK_MEDIUM_AND_ABOVE"
            }
        ]

        self.model = genai.GenerativeModel(
            model_name="gemini-pro",
            generation_config=generation_config,
            safety_settings=safety_settings,
        )

    def translate(self, code_snippet: str) -> str:
        query = QUERY.format(code_snippet=code_snippet)
        response = self.model.generate_content(query)
        return {"query": query, "result": response.result}


def build_chain(model_path: str, model_type: str, template: str):
    logging.info(f"Loading {model_path} ...")
    model = CTransformers(
        mode=model_path,
        model_type=model_type,
        config={
            "max_new_tokens": 1024,
            "temperature": 0.2,
            "context_length": 2048,
        },
    )
    prompt = PromptTemplate.from_template(template)
    chain = {"query": RunnablePassthrough()} | prompt | model | StrOutputParser()
    return chain


class CodeLlama:
    def __init__(self):
        self.llm_chain = build_chain(
            os.path.join(MODELS_DIR, "codellama-7b-instruct.Q2_K.gguf"),
            "llama",
            "<s>[INST] <<SYS>><</SYS>>\n{query}\nNo explanation is needed and wrap your code in ```. [/INST]",
        )

    def translate(self, code_snippet: str) -> Dict[str, str]:
        query = QUERY.format(code_snippet=code_snippet)
        result = self.llm_chain.invoke(query)
        return {"query": query, "result": result}
