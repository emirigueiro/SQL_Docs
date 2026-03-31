from pathlib import Path
from sqldocs.parser import parse_sql_documentation
from sqldocs.renderer import render_html


def main():
    import sys
    from .parser import parse_sql_documentation
    from .renderer import render_html

    input_file = sys.argv[1]

    doc = parse_sql_documentation(input_file)
    html = render_html(doc)

    print(html)