import marimo

__generated_with = "0.14.11"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    # substack reader history

    As far as I can tell, there's no way to download your Substack _consumer_ data. 
    There is an export function for a publication, but I wanted to play with my reading and interaction data
    (likes, restacks, and saves).

    Luckily, the API endpoint `/api/v1/reader/posts` returns all the information I was looking for.
    I threw together a notebook to mimic the reader app and build the dataset.

    It (seemingly) includes all the Substack posts a given user has opened while signed in,
    whether it was through the Reader app (mobile or [substack.com](https://substack.com)) or a particular publication's website (e.g. [avabear.xyz](https://avabear.xyz) or [seridescent.substack.com](https://seridescent.substack.com))

    The source is [on GitHub](https://github.com/seridescent/substack-reader-history-export). I wrote slightly more about this on [my website](https://seridescent.com/substack-history).

    Use the app view if you aren't playing with the code.
    """
    )
    return


@app.cell(hide_code=True)
def _(authed, credential_inputs, mo):
    mo.md(
        rf"""
    ## Credentials

    h.t. [nhagar/substack_api](https://www.nickhagar.net/substack_api/) for showing me how to make authenticated requests to Substack's API.

    See [How to Get Your Cookies](https://www.nickhagar.net/substack_api/authentication/#how-to-get-your-cookies)
    for instructions on getting credentials.

    {credential_inputs}

    {"✅ Auth session created!" if authed else "❌ Auth session not created"}
    """
    )
    return


@app.cell(hide_code=True)
def _(get_history_button, mo, raw_resps, user_request_limit):
    mo.md(
        rf"""
    ## Controls

    {user_request_limit}

    {get_history_button}

    {"Queried!" if raw_resps else "Click to start!"}
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Results

    You can download your data with the DataFrame viewer's "Download" tool at the bottom-right.
    """
    )
    return


@app.cell(hide_code=True)
def _(df, mo):
    mo.md("_Results not built_") if df is None else df
    return


@app.cell(hide_code=True)
def _(completion_threshold, df, mo):
    mo.stop(df is None)

    _completed_posts = df[
        df["max_read_progress"] >= (completion_threshold.value / 100)
    ]
    _unfinished_posts = df[
        df["max_read_progress"] < (completion_threshold.value / 100)
    ]

    mo.mermaid(f"""
    sankey-beta

    Posts opened,Completed,{len(_completed_posts)}
    Posts opened,Not finished,{len(_unfinished_posts)}
    Completed,Liked,{len(_completed_posts[_completed_posts["reaction"]])}
    Not finished,Liked,{len(_unfinished_posts[_unfinished_posts["reaction"]])}
    """)
    return


@app.cell(hide_code=True)
def _(completion_threshold, mo):
    mo.md(
        rf"""
    ### Sankey controls

    {completion_threshold}
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## implementation

    """
    )
    return


@app.cell
def _():
    import marimo as mo
    import pandas as pd
    import requests
    import json
    from datetime import datetime
    import time
    import itertools
    return datetime, itertools, mo, pd, requests, time


@app.cell
def _(requests):
    def make_authed_session(session_id, lli) -> requests.Session:
        """
        Open authenticated Substack session.

        h.t. https://www.nickhagar.net/substack_api/authentication/
        """
        session = requests.Session()

        session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        session.cookies.set(
            "substack.sid",
            session_id,
            domain=".substack.com",
            path="/",
            secure=True,
        )
        session.cookies.set(
            "substack.lli", lli, domain=".substack.com", path="/", secure=True
        )

        return session
    return (make_authed_session,)


@app.cell
def _(datetime, itertools, requests, time):
    def export_read_history(
        session: requests.Session,
        starting_from: datetime,
        spinner,
        request_interval_sec: float = 1,
        request_limit: int | None = None,
    ):
        after = starting_from
        resp_bodies = []
        collected_posts = 0
        for i in itertools.count():
            if request_limit and i >= request_limit:
                print(f"Reached request limit: {request_limit}")
                break

            params = {"inboxType": "seen", "limit": 20} | (
                {} if after is None else {"after": after.isoformat()}
            )

            resp = session.get(
                url="https://substack.com/api/v1/reader/posts", params=params
            )

            # Check for response errors
            if resp.status_code != 200:
                print(f"Error: HTTP {resp.status_code} - {resp.text}")
                break

            try:
                resp_json = resp.json()
            except Exception as e:
                print(f"Error parsing JSON response: {e}")
                break

            resp_bodies.append(resp_json)

            collected_posts += len(resp_json["posts"])
            spinner.update(subtitle=f"Collected {collected_posts} posts")

            if not resp_json["more"]:
                print(f"no more posts")
                break

            after = min(
                datetime.fromisoformat(post["inboxItem"]["seen_at"])
                for post in resp_json["posts"]
            )

            time.sleep(request_interval_sec)

        spinner.update(subtitle="Done!")
        return resp_bodies
    return (export_read_history,)


@app.cell
def _():
    TOP_LEVEL_POST_SCHEMA = {
        "title": "string",
        "canonical_url": "string",
        "max_read_progress": "float64",
        "reaction_count": "int64",
        "subtitle": "string",
        "type": "string",
        "id": "int64",
        "publication_id": "int64",
        "slug": "string",
        "post_date": "datetime64[ns]",
        "audience": "string",
        "podcast_duration": "timedelta",  # nullable
        "video_upload_id": "string",  # nullable
        "is_published": "boolean",
        "restacks": "int64",
        "cover_image_is_explicit": "boolean",
        "description": "string",
        "wordcount": "int64",
        "postTags": "object",  # list of strings
        "reaction": "boolean",
        "comment_count": "int64",
        "child_comment_count": "int64",
        "is_geoblocked": "boolean",
        "is_saved": "boolean",
        "saved_at": "datetime64[ns]",  # nullable
        "is_viewed": "boolean",
        "read_progress": "float64",
        "audio_progress": "float64",
        "max_audio_progress": "float64",
        "video_progress": "float64",
        "max_video_progress": "float64",
        "restacked": "boolean",
    }

    BYLINE_SCHEMA = {
        "id": "int64",
        "name": "string",
        "handle": "string",
        "is_guest": "boolean",
    }

    POST_SCHEMA = TOP_LEVEL_POST_SCHEMA | {
        "published_bylines": "object",  # list of dicts with BYLINE_SCHEMA structure
        "seen_at": "datetime64[ns]",
    }
    return BYLINE_SCHEMA, POST_SCHEMA, TOP_LEVEL_POST_SCHEMA


@app.cell
def _(BYLINE_SCHEMA, POST_SCHEMA, TOP_LEVEL_POST_SCHEMA, pd):
    def normalize_post(raw_post):
        top_levels = {
            k: raw_post[k]
            for k in raw_post.keys()
            if k in TOP_LEVEL_POST_SCHEMA.keys()
        }

        published_bylines = [
            {k: byline[k] for k in byline.keys() if k in BYLINE_SCHEMA.keys()}
            for byline in raw_post["publishedBylines"]
        ]

        seen_at = raw_post["inboxItem"]["seen_at"]

        return top_levels | {
            "published_bylines": published_bylines,
            "seen_at": seen_at,
        }


    def responses_to_df(responses) -> pd.DataFrame:
        data = [
            normalize_post(raw_post)
            for resp in responses
            for raw_post in resp["posts"]
        ]

        df = pd.DataFrame(data)

        # Apply the schema by casting data types
        for col, dtype in POST_SCHEMA.items():
            if col not in df.columns:
                print(f"Did not find column {col} in {df.columns=}")

            if dtype == "datetime64[ns]":
                df[col] = pd.to_datetime(df[col])
            elif dtype == "timedelta":
                df[col] = pd.to_timedelta(df[col], unit="s")
            else:
                df[col] = df[col].astype(dtype)

        # Reorder columns based on schema order
        return df[list(POST_SCHEMA.keys())]
    return (responses_to_df,)


@app.cell
def _(mo):
    user_substack_sid = mo.ui.text(
        placeholder="Your substack.sid cookie value", label="`substack.sid`"
    )
    user_substack_lli = mo.ui.text(
        placeholder="Your substack.lli cookie value", label="`substack.lli`"
    )
    credential_inputs = mo.vstack([user_substack_sid, user_substack_lli])
    return credential_inputs, user_substack_lli, user_substack_sid


@app.cell
def _(make_authed_session, user_substack_lli, user_substack_sid):
    if user_substack_sid.value and user_substack_lli.value:
        authed = make_authed_session(
            user_substack_sid.value, user_substack_lli.value
        )
    else:
        authed = None
    return (authed,)


@app.cell
def _(mo):
    completion_threshold = mo.ui.number(
        start=0, stop=100, value=90, label="Percentage read to consider complete:"
    )
    return (completion_threshold,)


@app.cell(hide_code=True)
def _(
    authed,
    datetime,
    export_read_history,
    get_history_button,
    mo,
    user_request_limit,
):
    if get_history_button.value:
        with mo.status.spinner(title="Querying history") as _spinner:
            raw_resps = export_read_history(
                authed,
                datetime.now(),
                _spinner,
                request_limit=user_request_limit.value,
            )
    else:
        raw_resps = None
    return (raw_resps,)


@app.cell
def _(authed, mo):
    get_history_button = mo.ui.run_button(
        label="Start querying history", disabled=authed is None
    )
    user_request_limit = mo.ui.number(step=1, label="Optional request limit: ")
    return get_history_button, user_request_limit


@app.cell
def _(raw_resps, responses_to_df):
    if raw_resps:
        df = responses_to_df(raw_resps)
    else:
        df = None
    return (df,)


if __name__ == "__main__":
    app.run()
