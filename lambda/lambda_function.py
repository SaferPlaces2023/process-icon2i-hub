from process_icon2i_hub import parse_event
from process_icon2i_hub import run_icon2i_ingestor, run_icon2i_retriever


def ingestor_handler(event, context):
    """
    ingestor_handler - lambda function for the ICON-2I ingestor
    """
    kwargs = parse_event(event, run_icon2i_ingestor)

    res = run_icon2i_ingestor(**kwargs)

    return {
        "statusCode": 200,
        "body": {
            "result": res
        }
    }


def retriever_handler(event, context):
    """
    retriever_handler - lambda function for the ICON-2I retriever
    """
    kwargs = parse_event(event, run_icon2i_retriever)

    res = run_icon2i_retriever(**kwargs)

    return {
        "statusCode": 200,
        "body": {
            "result": res
        }
    }


if __name__ == "__main__":
    ingestor_event = {
        "variable": "total_precipitation",
        "debug": "true"
    }
    print(ingestor_handler(ingestor_event, None))

    retriever_event = {
        "lat_range": [43.92, 44.77],
        "long_range": [12.20, 12.83],
        "time_range": ["2025-01-21T08:00:00", "2025-01-22T23:00:00"],
        "variable": "total_precipitation",
        "debug": "true"
    }
    print(retriever_handler(retriever_event, None))
