from typing import List, Union


def preprocess(text: str) -> List[str]:
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
        if x == "":
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
