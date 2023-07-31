from typing import List, Union


def preprocess(text: str) -> List[str]:
    """
    Preprocess the text to split the code by macros and procs.

    Args:
        text (str): The text to preprocess.

    Returns:
        List[str]: The preprocessed text.
    """
    rows = text.split("\n")
    rows_split = []
    for rows0 in split(rows, "%macro", "%mend"):
        rows_split.extend(split(rows0, "proc", ["quit", "run"]))

    subtexts = ["\n".join(x) for x in rows_split]
    return subtexts


def split(
    rows: List[str],
    start: Union[str, List[str]],
    end: Union[str, List[str]],
) -> List[List[str]]:
    """Splits a list of rows into sublists based on a start and end string.

    The start and end strings can be either a single string or a list of strings.
    If a single string is provided, it will be used for both the start and end
    strings.

    Args:
        rows: The list of rows to split.
        start: The start string or list of strings.
        end: The end string or list of strings.

    Returns:
        A list of sublists of rows.
    """
    if isinstance(start, str):
        start = [start]
    else:
        if not isinstance(start, list):
            raise ValueError("'start' must be either str or list[str]")

    if isinstance(end, str):
        end = [end]
    else:
        if not isinstance(end, list):
            raise ValueError("'end' must be either str or list[str]")

    subtexts = []
    tmp = []
    for x in rows:
        if x.strip() == "":
            continue
        if any([s in x.lower() for s in start]):
            if len(tmp) > 0:
                subtexts.append(tmp)
            tmp = [x]
        elif any([e in x.lower() for e in end]):
            tmp.append(x)
            subtexts.append(tmp)
            tmp = []
        else:
            tmp.append(x)

    if len(tmp) > 0:
        subtexts.append(tmp)
    return subtexts
