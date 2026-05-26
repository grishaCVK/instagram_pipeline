import json
from datetime import date, timedelta

import requests

import config


BASE_URL = f"https://graph.facebook.com/{config.GRAPH_API_VERSION}"


FIELDS = [
    "date_start",
    "date_stop",

    "campaign_id",
    "campaign_name",
    "adset_id",
    "adset_name",
    "ad_id",
    "ad_name",
    "objective",

    "spend",
    "impressions",
    "reach",
    "frequency",
    "cpm",

    "clicks",
    "inline_link_clicks",
    "inline_link_click_ctr",
    "ctr",
    "cpc",

    "actions",
    "cost_per_action_type",
]


def graph_get(endpoint: str, params: dict) -> dict:
    params = dict(params)
    params["access_token"] = config.META_ACCESS_TOKEN

    response = requests.get(
        f"{BASE_URL}{endpoint}",
        params=params,
        timeout=60,
    )

    try:
        data = response.json()
    except ValueError:
        raise RuntimeError(response.text)

    if response.status_code != 200 or "error" in data:
        raise RuntimeError(json.dumps(data, ensure_ascii=False, indent=2))

    return data


def test_breakdown(
    *,
    name: str,
    breakdowns: str,
    date_since: str,
    date_until: str,
) -> None:
    print("=" * 100)
    print(f"TEST: {name}")
    print(f"date={date_since} -> {date_until}")
    print(f"breakdowns={breakdowns}")
    print("=" * 100)

    endpoint = f"/{config.AD_ACCOUNT_ID}/insights"

    params = {
        "fields": ",".join(FIELDS),
        "level": "ad",
        "time_increment": 1,
        "breakdowns": breakdowns,
        "limit": 10,
        "time_range": json.dumps(
            {
                "since": date_since,
                "until": date_until,
            }
        ),
    }

    data = graph_get(endpoint, params)
    rows = data.get("data", [])

    print(f"RESULT: OK | rows_found={len(rows)}")

    if not rows:
        return

    first_row = rows[0]

    print("COLUMNS RETURNED:")
    print(sorted(first_row.keys()))

    print("FIRST ROW:")
    print(json.dumps(first_row, ensure_ascii=False, indent=2))


def main() -> None:
    date_until = (date.today() - timedelta(days=1)).isoformat()
    date_since = (date.today() - timedelta(days=7)).isoformat()

    tests = [
        {
            "name": "geo_country_region",
            "breakdowns": "country,region",
        },
        {
            "name": "device_platform_impression_device",
            "breakdowns": "device_platform,impression_device",
        },
    ]

    for test in tests:
        try:
            test_breakdown(
                name=test["name"],
                breakdowns=test["breakdowns"],
                date_since=date_since,
                date_until=date_until,
            )
        except Exception as error:
            print("=" * 100)
            print(f"RESULT: FAILED | {test['name']}")
            print(error)


if __name__ == "__main__":
    main()