import os

import argparse
from tqdm import tqdm

from src.preprocess import preprocess
from src.translate import Gemini

MIN_LENGTH = 10

LLM = Gemini()


def code_convert(input_filename: str, output_filename: str) -> None:
    """Converts code to Python file.

    Args:
        input_filename (str): The path to the input file.
        output_filename (str): The path to the output file.
    """
    with open(input_filename, "r") as f:
        text = f.read()

    subtexts = preprocess(text)

    cleaned = """# boilerplate code for spark
from src.utils.spark_conf import get_spark_sql_context

_, sql_context = get_spark_sql_context(
    app_name="my_app"
)

"""

    for i in tqdm(range(len(subtexts)), desc="Translating"):
        snippet = subtexts[i]
        cleaned += f'"""[original]\n{snippet}\n"""\n'

        if len(snippet) > MIN_LENGTH:
            translated = LLM.translate(snippet)
        else:
            translated = -1

        if translated is None or translated == "":
            cleaned += f'"""[unable to convert]\n{snippet}\n"""\n\n'
        elif translated == -1:
            cleaned += f'"""[not converted, too short]\n{snippet}\n"""\n\n'
        else:
            cleaned += f'"""[converted]\n{snippet}\n"""\n'
            z = translated.replace("```sql", "").replace("```", "")
            cleaned += 'sql_context.sql("""' + z + '""")\n\n'

    with open(output_filename, "w") as f:
        f.write(cleaned)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input_filename", type=str)
    parser.add_argument("-o", "--output_filename", default=None, type=str)
    args = parser.parse_args()

    input_filename = args.input_filename
    output_filename = args.output_filename
    if output_filename is None:
        output_filename = os.path.splitext(input_filename)[0] + "_translated.py"

    code_convert(input_filename, output_filename)
