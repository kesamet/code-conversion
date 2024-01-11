import re
from typing import List, Union


def preprocess(text: str) -> List[str]:
    """Preprocess input code into snippets."""
    # Remove comments
    lines = [x.rstrip() for x in text.split("\n") if x.rstrip()]
    lines = remove_comments(lines)

    # Split by macros and procs
    groups_macro = group_lines(lines, "%macro", "%mend")
    groups_final = []
    _group = []
    level_macro = 0
    for group in groups_macro:
        first_line = group[0].strip().lower()
        last_line = group[-1].strip().lower()
        if first_line.startswith("%macro") or level_macro > 0:
            if first_line.startswith("%macro"):
                level_macro += 1
            _group.extend(group)
            if "%mend" in last_line:
                level_macro -= 1
                if level_macro == 0:
                    groups_final.append(_group)
                    _group = []
        else:
            groups_proc = group_lines(group, ["proc", "data"], ["quit;", "run;"])
            groups_split = split_single_line_by_keywords(groups_proc)
            groups_final.extend(groups_split)

    text_split = ["\n".join(x) for x in groups_final]
    return text_split


def remove_comments(lines: List[str]) -> List[str]:
    """Remove comments from a list of lines."""
    lines_cleaned = []
    is_comment = False
    for line in lines:
        if line.strip().startswith("/*"):
            is_comment = True
        if is_comment:
            if line.strip().endswith("*/"):
                is_comment = False
        else:
            lines_cleaned.append(line)

    # remove any other comments embedded in a line
    lines_cleaned = [re.sub(r"/\*.*?\*/", "", s) for s in lines_cleaned]
    return lines_cleaned


def group_lines(
    lines: List[str],
    start: Union[str, List[str]],
    end: Union[str, List[str]],
) -> List[List[str]]:
    """Groups a list of lines into sublists based on a start and end string.

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

    lst = []
    tmp = []
    for line in lines:
        if line.strip() == "":
            continue
        if any([line.lstrip().startswith(s) for s in start]):
            if len(tmp) > 0:
                lst.append(tmp)
            tmp = [line]
        elif any([e in line.lower() for e in end]):
            tmp.append(line)
            lst.append(tmp)
            tmp = []
        else:
            tmp.append(line)

    if len(tmp) > 0:
        lst.append(tmp)
    return lst


def split_single_line_by_keywords(groups: List[List[str]]) -> List[List[str]]:
    """Splits a single line that starts with a keyword."""
    keywords = ["%put", "%let", "libname"]

    groups_split = []
    for group in groups:
        tmp = []
        for line in group:
            if any([line.lstrip().startswith(s) for s in keywords]):
                if len(tmp) > 0:
                    groups_split.append(tmp)
                    tmp = []
                groups_split.append([line])
            else:
                tmp.append(line)
        if len(tmp) > 0:
            groups_split.append(tmp)
    return groups_split
