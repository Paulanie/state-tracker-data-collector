import logging
import azure.functions as func
from dotenv import load_dotenv

load_dotenv()

from shared.functions import amendments

app = func.FunctionApp()


@app.function_name(name="amendments")
@app.schedule(schedule="*/5 * * * * *", arg_name="mytimer", run_on_startup=True)
def load_amendments(mytimer: func.TimerRequest) -> None:
    amendments()


if __name__ == '__main__':
    from shared.utils.dev import dev_with_pycharm

    # dev_with_pycharm()

    logging.getLogger().setLevel(logging.INFO)
    amendments()
