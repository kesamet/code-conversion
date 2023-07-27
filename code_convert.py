import logging
import os

import argparse
from tqdm import tqdm
from src.preprocess import preprocess
from src.translate import translate

logging.basicConfig(level=logging.INFO)


def code_convert(input_filename: str, output_filename: str) -> None:
    with open(input_filename, "r") as f:
        text = f.read()

    subtexts = preprocess(text)

    cleaned = ""
    for i in tqdm(range(len(subtexts)), desc="Translating"):
        if len(subtexts[i]) > 10:
            translated = translate(subtexts[i])
        else:
            translated = None

        if translated is None or translated == "":
            cleaned += f'"""[unable to convert]\n{subtexts[i]}\n"""\n\n'
        else:
            cleaned += f'"""[converted]\n{subtexts[i]}\n"""\n'
            z = translated.replace("```sql", "").replace("```", "")
            cleaned += 'spark.sql("""' + z + '""")\n\n'

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
