from typing import Tuple


def extract_code(llm_output: str) -> Tuple[str, str]:
    """Extracts converted code from LLM output."""
    try:
        code = llm_output.split("```")[1].strip()
        if code.startswith("sql"):
            return code.replace("sql", "").strip(), "sql"
        if code.startswith("python"):
            return "# " + code, "python"
        if code.startswith("scala"):
            return "# " + code, "scala"
        return code, "others"
    except Exception:
        return None, None
