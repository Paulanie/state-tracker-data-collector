import logging
import azure.functions as func
from dotenv import load_dotenv

load_dotenv()

from shared.functions import amendments, deputies

app = func.FunctionApp()


@app.function_name(name="amendments")
@app.schedule(schedule="*/5 * * * * *", arg_name="mytimer", run_on_startup=True)
def load_amendments(mytimer: func.TimerRequest) -> None:
    amendments()


@app.function_name(name="deputies")
@app.schedule(schedule="*/5 * * * * *", arg_name="mytimer", run_on_startup=True)
def load_deputies(mytimer: func.TimerRequest) -> None:
    deputies()

if __name__ == '__main__':
    from shared.utils.dev import dev_with_pycharm

    # dev_with_pycharm()

    logging.getLogger().setLevel(logging.INFO)
    #amendments()
    deputies()
