from pathlib import Path
from parser import parse_sql_documentation
from renderer import render_html


def generate_doc(sql_path: str) -> str:
    # Leer SQL
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()

    # Parsear
    doc = parse_sql_documentation(sql)

    # Ruta relativa al template
    base_path = Path(__file__).parent
    template_path = base_path / "templates"

    # Renderizar
    html = render_html(doc, template_path=str(template_path))

    return html