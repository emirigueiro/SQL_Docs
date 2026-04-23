from .parser import parse_sql_documentation
from .renderer import render_html

def generate_doc(input_file):
    doc = parse_sql_documentation(input_file)
    html = render_html(doc)
    return html

__all__ = ["generate_doc"]