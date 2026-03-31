from jinja2 import Environment, BaseLoader
from importlib import resources


def load_template(template_name="template.html"):
    return (
        resources.files(__package__)
        .joinpath("templates", template_name)
        .read_text(encoding="utf-8")
    )


def render_html(doc, template_name="template.html", custom_template_str=None):
    if custom_template_str:
        template_str = custom_template_str
    else:
        template_str = load_template(template_name)

    env = Environment(loader=BaseLoader())
    template = env.from_string(template_str)

    return template.render(doc=doc)