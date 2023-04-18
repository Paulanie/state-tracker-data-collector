import datetime
import logging
import azure.functions as func

from shared.functions import amendments

app = func.FunctionApp()


@app.function_name(name="amendments")
@app.schedule(schedule="*/5 * * * * *", arg_name="mytimer", run_on_startup=True)
def test_function(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    amendments()


if __name__ == '__main__':
    from dotenv import load_dotenv
    from shared.utils.dev import dev_with_pycharm

    load_dotenv()
    # dev_with_pycharm()

    logging.getLogger().setLevel(logging.INFO)
    amendments()
