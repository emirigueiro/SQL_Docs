import re
import yaml


# -----------------------------
# 🔹 EXTRACT SECTIONS
# -----------------------------
def extract_section(sql: str, section_name: str) -> str:
    pattern = rf"/\*\s*{section_name}\s*\*/(.*?)(?=/\*|\Z)"
    match = re.search(pattern, sql, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else ""


def clean_yaml_lines(section: str) -> str:
    lines = section.splitlines()
    cleaned = [re.sub(r"^\s*--\s?", "", line) for line in lines if line.strip()]
    return "\n".join(cleaned)


def parse_yaml_section(sql: str, section_name: str):
    raw = extract_section(sql, section_name)
    yaml_text = clean_yaml_lines(raw)

    if not yaml_text:
        return []

    try:
        data = yaml.safe_load(yaml_text)

        # 🔥 Aseguramos siempre lista (clave para Jinja)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return [data]
        else:
            return []

    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing {section_name}: {e}")


# -----------------------------
# 🔹 PROCESS COMMENTS
# -----------------------------
def parse_process_comments(sql: str):

    # Normalización
    sql = sql.replace('--LC:', '-- LC:')
    sql = sql.replace('--NT:', '-- NT:')

    pattern = r"--\s*(STEP[^:]*|LC|NT)\s*:\s*(.+)"
    matches = list(re.finditer(pattern, sql, re.IGNORECASE))

    results = []

    for idx, match in enumerate(matches, start=1):
        clase_raw = match.group(1).strip()
        comment = match.group(2).strip()

        # 👇 Mantiene "Step 1", "Step 2", etc.
        if clase_raw.upper().startswith("STEP"):
            clase = re.sub(r"\s+", " ", clase_raw).title()
        elif clase_raw.upper() == "LC":
            clase = "Line Comment"
        elif clase_raw.upper() == "NT":
            clase = "Note"
        else:
            clase = clase_raw

        line_number = sql[:match.start()].count("\n") + 1

        results.append({
            "order": idx,
            "class": clase,
            "comment": comment,
            "line": line_number
        })

    return results


# -----------------------------
# 🔹 MAIN PARSER
# -----------------------------
def parse_sql_documentation(sql: str) -> dict:
    return {
        "summary": parse_yaml_section(sql, "SUMMARY"),
        "related_programs": parse_yaml_section(sql, "RELATED PROGRAMS"),
        "sources": parse_yaml_section(sql, "SOURCES"),
        "products": parse_yaml_section(sql, "PRODUCTS"),
        "versions": parse_yaml_section(sql, "HISTORICAL VERSIONS"),
        "process_comments": parse_process_comments(sql),
    }


