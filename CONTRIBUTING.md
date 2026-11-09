# Contributing

Bug reports, feed compatibility issues, and pull requests are welcome.

## Setup

```sh
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest
```

Spark needs Java 17 or 21 on `JAVA_HOME`.

For the AWS path, start LocalStack with `docker compose up -d` and run `LOCALSTACK=1 pytest tests/integration`.

## Reporting a feed that doesn't work

Please include the feed URL, the static GTFS URL, and the output of `compute`. If the feed needs a key, say which agency it is instead of sharing the key.

## Pull requests

- Add a test for the behavior you change. The simulator in `tests/sim.py` builds vehicles with known delays.
- Run `terraform fmt` if you touch `infra/terraform`.
