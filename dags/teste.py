from pathlib import Path

from airflow import DAG
from airflow.models import Param
from airflow.models.param import ParamsDict
from airflow.utils.trigger_rule import TriggerRule

from airflow.decorators import dag, task
from pendulum import datetime

with DAG(
        dag_id=Path(__file__).stem,
        dag_display_name="Params Trigger UI",
        doc_md=__doc__,
        schedule=None,
        start_date=datetime(2022, 3, 4),
        catchup=False,
        tags=["example", "params"],
        params={
            "names": Param(
                ["Linda", "Martha", "Thomas"],
                type="array",
                description="Define the list of names for which greetings should be generated in the logs."
                            " Please have one name per line.",
                title="Names to greet",
            ),
            "english": Param(True, type="boolean", title="English"),
            "german": Param(True, type="boolean", title="German (Formal)"),
            "french": Param(True, type="boolean", title="French"),
            # Let's start simple: Standard dict values are detected from type and offered as entry form fields.
            # Detected types are numbers, text, boolean, lists and dicts.
            # Note that such auto-detected parameters are treated as optional (not required to contain a value)
            "x": 3,
            "text": "Hello World!",
            "flag": False,
            "a_simple_list": ["one", "two", "three", "actually one value is made per line"],
            # But of course you might want to have it nicer! Let's add some description to parameters.
            # Note if you can add any Markdown formatting to the description, you need to use the description_md
            # attribute.
            "most_loved_number": Param(
                42,
                type="integer",
                title="Your favorite number",
                description_md="Everybody should have a **favorite** number. Not only _math teachers_. "
                               "If you can not think of any at the moment please think of the 42 which is very famous because"
                               "of the book [The Hitchhiker's Guide to the Galaxy]"
                               "(https://en.wikipedia.org/wiki/Phrases_from_The_Hitchhiker%27s_Guide_to_the_Galaxy#"
                               "The_Answer_to_the_Ultimate_Question_of_Life,_the_Universe,_and_Everything_is_42).",
            ),
            # If you want to have a selection list box then you can use the enum feature of JSON schema
            "pick_one": Param(
                "value 42",
                type="string",
                title="Select one Value",
                description="You can use JSON schema enum's to generate drop down selection boxes.",
                enum=[f"value {i}" for i in range(16, 64)]
            ),
            "required_field": Param(
                # In this example we have no default value
                # Form will enforce a value supplied by users to be able to trigger
                type="string",
                title="Required text field",
                description="This field is required. You can not submit without having text in here.",
            ),
            "optional_field": Param(
                "optional text, you can trigger also w/o text",
                type=["null", "string"],
                title="Optional text field",
                description_md="This field is optional. As field content is JSON schema validated you must "
                               "allow the `null` type.",
            ),
        },
) as dag:
    @task(task_id="get_names", task_display_name="Get names")
    def get_names(**kwargs) -> list[str]:
        params: ParamsDict = kwargs["params"]
        if "names" not in params:
            print("Uuups, no names given, was no UI used to trigger?")
            return []
        return params["names"]


    @task.branch(task_id="select_languages", task_display_name="Select languages")
    def select_languages(**kwargs) -> list[str]:
        params: ParamsDict = kwargs["params"]
        selected_languages = []
        for lang in ["english", "german", "french"]:
            if params[lang]:
                selected_languages.append(f"generate_{lang}_greeting")
        return selected_languages


    @task(task_id="generate_english_greeting", task_display_name="Generate English greeting")
    def generate_english_greeting(name: str) -> str:
        return f"Hello {name}!"


    @task(task_id="generate_german_greeting", task_display_name="Erzeuge Deutsche Begrüßung")
    def generate_german_greeting(name: str) -> str:
        return f"Sehr geehrter Herr/Frau {name}."


    @task(task_id="generate_french_greeting", task_display_name="Produire un message d'accueil en français")
    def generate_french_greeting(name: str) -> str:
        return f"Bonjour {name}!"


    @task(task_id="print_greetings", task_display_name="Print greetings", trigger_rule=TriggerRule.ALL_DONE)
    def print_greetings(greetings1, greetings2, greetings3) -> None:
        for g in greetings1 or []:
            print(g)
        for g in greetings2 or []:
            print(g)
        for g in greetings3 or []:
            print(g)
        if not (greetings1 or greetings2 or greetings3):
            print("sad, nobody to greet :-(")


    lang_select = select_languages()
    names = get_names()
    english_greetings = generate_english_greeting.expand(name=names)
    german_greetings = generate_german_greeting.expand(name=names)
    french_greetings = generate_french_greeting.expand(name=names)
    lang_select >> [english_greetings, german_greetings, french_greetings]
    results_print = print_greetings(english_greetings, german_greetings, french_greetings)
