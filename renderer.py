from jinja2 import Environment, FileSystemLoader
from pathlib import Path


def render_html(doc, template_path=None, template_name="template.html"):

    if template_path is None:
        template_path = Path(__file__).parent / "templates"

    env = Environment(loader=FileSystemLoader(str(template_path)))
    template = env.get_template(template_name)

    return template.render(doc=doc)