import os

import google.generativeai as palm

palm.configure(api_key=os.environ["PALM_API_KEY"])

DEFAULTS = {
    "model": "models/text-bison-001",
    "temperature": 0.0,
    "candidate_count": 1,
    "top_k": 40,
    "top_p": 0.95,
    "max_output_tokens": 1024,
    "stop_sequences": [],
    "safety_settings": [
        {"category": "HARM_CATEGORY_DEROGATORY", "threshold": 1},
        {"category": "HARM_CATEGORY_TOXICITY", "threshold": 1},
        {"category": "HARM_CATEGORY_VIOLENCE", "threshold": 2},
        {"category": "HARM_CATEGORY_SEXUAL", "threshold": 2},
        {"category": "HARM_CATEGORY_MEDICAL", "threshold": 2},
        {"category": "HARM_CATEGORY_DANGEROUS", "threshold": 2},
    ],
}

PROMPT = """How do I translate the following SAS code, delimited by triple backticks, to SQL?
```{}```"""


def translate(text: str) -> str:
    """Translates SAS code snippet to SQL.

    Args:
        text (str): The code to translate.

    Returns:
        str: The translated code.
    """
    response = palm.generate_text(prompt=PROMPT.format(text), **DEFAULTS)
    return response.result
