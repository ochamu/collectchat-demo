"""Streamlitデモアプリ: チャンネル横断の共起ユーザー分析."""

from __future__ import annotations

import io
import os
import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple, Mapping

import pandas as pd
import streamlit as st

import clickhouse_connect
import altair as alt


@dataclass(frozen=True)
class ClickHouseConfig:
    """ClickHouse接続情報を保持するための設定クラス."""

    host: str
    port: int
    user: str
    password: str
    database: str
    secure: bool


DEFAULT_TABLE = os.getenv("CLICKHOUSE_TABLE", "youtube_live_chat")


def _load_setting(key: str, default: Optional[str] = None) -> Optional[str]:
    """環境変数とStreamlit secretsから設定値を読み出す."""

    secrets_section: Dict[str, str] = {}
    try:
        secrets_root = st.secrets  # type: ignore[attr-defined]
        candidate = secrets_root["clickhouse"]  # type: ignore[index]
    except (AttributeError, KeyError, TypeError):
        candidate = None
    except Exception:
        candidate = None

    if isinstance(candidate, Mapping):
        secrets_section = {
            str(k): str(v)
            for k, v in candidate.items()
            if v not in (None, "")
        }

    if not secrets_section:
        local_path = Path(".streamlit/secrets.toml")
        if local_path.exists():
            decode_error = getattr(tomllib, "TOMLDecodeError", ValueError)
            try:
                with local_path.open("rb") as fp:
                    data = tomllib.load(fp)
                candidate = data.get("clickhouse", {})
                if isinstance(candidate, Mapping):
                    secrets_section = {
                        str(k): str(v)
                        for k, v in candidate.items()
                        if v not in (None, "")
                    }
            except (OSError, decode_error):
                secrets_section = {}

    if key in secrets_section and secrets_section[key] not in (None, ""):
        return str(secrets_section[key])
    env_key = f"CLICKHOUSE_{key.upper()}"
    value = os.getenv(env_key)
    if value:
        return value
    return default


@st.cache_resource(show_spinner=False)
def get_client(config: ClickHouseConfig) -> clickhouse_connect.driver.Client:
    """ClickHouseクライアントを生成する."""

    return clickhouse_connect.get_client(
        host=config.host,
        port=config.port,
        username=config.user,
        password=config.password,
        database=config.database,
        secure=config.secure,
    )


@st.cache_data(show_spinner=False)
def fetch_channels(
    config: ClickHouseConfig,
    table: str,
    start_ts: dt.datetime,
    end_ts: dt.datetime,
) -> List[str]:
    """期間内に登場したチャンネル名を取得する."""

    sql = f"""
        SELECT DISTINCT channel_name
        FROM {table}
        WHERE channel_name != ''
          AND message_timestamp BETWEEN %(start)s AND %(end)s
        ORDER BY channel_name
        LIMIT 10000
    """
    params = {"start": start_ts, "end": end_ts}
    result = get_client(config).query(sql, parameters=params)
    return [row[0] for row in result.result_rows]


@st.cache_data(show_spinner=False)
def fetch_channel_pairs(
    config: ClickHouseConfig,
    table: str,
    start_ts: dt.datetime,
    end_ts: dt.datetime,
    channel_filter: Sequence[str],
    min_messages: int,
    min_shared_users: int,
    limit: int,
) -> pd.DataFrame:
    """共起チャンネルの組み合わせを集計する."""

    filters = ["message_timestamp BETWEEN %(start)s AND %(end)s", "channel_name != ''"]
    params: Dict[str, object] = {
        "start": start_ts,
        "end": end_ts,
        "min_messages": min_messages,
        "min_shared": min_shared_users,
        "limit": limit,
    }
    if channel_filter:
        filters.append("channel_name IN %(channels)s")
        params["channels"] = tuple(channel_filter)

    filter_sql = " AND ".join(filters)
    sql = f"""
        WITH filtered AS (
            SELECT
                COALESCE(author_channel_id, author_name) AS author_key,
                channel_name,
                count() AS message_count
            FROM {table}
            WHERE {filter_sql}
            GROUP BY author_key, channel_name
            HAVING message_count >= %(min_messages)s
        ),
        user_channels AS (
            SELECT
                author_key,
                arraySort(groupArray(channel_name)) AS channel_list
            FROM filtered
            GROUP BY author_key
            HAVING length(channel_list) > 1
        ),
        pairs AS (
            SELECT
                author_key,
                arrayJoin(
                    arrayFlatten(
                        arrayMap(
                            i -> arrayMap(
                                val -> tuple(channel_list[i], val),
                                arraySlice(channel_list, i + 1)
                            ),
                            arrayEnumerate(channel_list)
                        )
                    )
                ) AS pair
            FROM user_channels
        )
        SELECT
            pair.1 AS channel_a,
            pair.2 AS channel_b,
            count() AS shared_users
        FROM pairs
        GROUP BY pair
        HAVING shared_users >= %(min_shared)s
        ORDER BY shared_users DESC, channel_a, channel_b
        LIMIT %(limit)s
    """

    result = get_client(config).query(sql, parameters=params)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    return df


@st.cache_data(show_spinner=False)
def fetch_pair_details(
    config: ClickHouseConfig,
    table: str,
    start_ts: dt.datetime,
    end_ts: dt.datetime,
    channel_a: str,
    channel_b: str,
    min_messages: int,
    limit: int,
) -> pd.DataFrame:
    """特定のチャンネルペアについて共通ユーザーの詳細を取得する."""

    sql = f"""
        WITH base AS (
            SELECT
                COALESCE(author_channel_id, author_name) AS author_key,
                argMax(author_name, message_timestamp) AS display_name,
                countIf(channel_name = %(a)s) AS messages_in_a,
                countIf(channel_name = %(b)s) AS messages_in_b
            FROM {table}
            WHERE channel_name IN (%(a)s, %(b)s)
              AND channel_name != ''
              AND message_timestamp BETWEEN %(start)s AND %(end)s
            GROUP BY author_key
        )
        SELECT
            author_key,
            display_name,
            messages_in_a,
            messages_in_b,
            messages_in_a + messages_in_b AS total_messages
        FROM base
        WHERE messages_in_a >= %(min_messages)s AND messages_in_b >= %(min_messages)s
        ORDER BY total_messages DESC
        LIMIT %(limit)s
    """

    params = {
        "a": channel_a,
        "b": channel_b,
        "start": start_ts,
        "end": end_ts,
        "min_messages": min_messages,
        "limit": limit,
    }
    result = get_client(config).query(sql, parameters=params)
    return pd.DataFrame(result.result_rows, columns=result.column_names)


def _infer_bool(value: Optional[str], default: bool = True) -> bool:
    """文字列から簡易的に真偽値を推定する."""

    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _prepare_config() -> Optional[ClickHouseConfig]:
    """接続設定を読み込み、欠落時はエラーメッセージを表示する."""

    host = _load_setting("host")
    user = _load_setting("user", "default")
    password = _load_setting("password", "")
    database = _load_setting("database", "default")
    port_raw = _load_setting("port", "8443")
    secure_raw = _load_setting("secure", "true")

    if not host:
        st.error("ClickHouseのホスト名が設定されていません。secrets.tomlまたは環境変数を確認してください。")
        return None

    try:
        port = int(port_raw) if port_raw is not None else 8443
    except ValueError:
        st.error("ポート番号が数値として解釈できません。")
        return None

    secure = _infer_bool(secure_raw, default=True)

    return ClickHouseConfig(
        host=host,
        port=port,
        user=user or "default",
        password=password or "",
        database=database or "default",
        secure=secure,
    )


def _default_dates() -> Tuple[dt.date, dt.date]:
    """日付フィルターの初期値を返す（直近7日間）。"""

    today = dt.date.today()
    return today - dt.timedelta(days=7), today


def _to_utc_range(start_date: dt.date, end_date: dt.date) -> Tuple[dt.datetime, dt.datetime]:
    """日付入力をUTCのDatetime範囲へ変換する."""

    start_ts = dt.datetime.combine(start_date, dt.time.min, tzinfo=dt.timezone.utc)
    end_ts = dt.datetime.combine(end_date, dt.time.max, tzinfo=dt.timezone.utc)
    return start_ts, end_ts


def _download_csv(df: pd.DataFrame) -> bytes:
    """DataFrameをCSV形式でエクスポートする."""

    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    return buffer.getvalue().encode("utf-8")


def _build_heatmap_dataframe(pair_df: pd.DataFrame, limit: int) -> pd.DataFrame:
    """ヒートマップ描画用にチャンネルペアのデータを整形する."""

    if pair_df.empty:
        return pair_df

    limited = pair_df.nlargest(limit, "shared_users") if limit < len(pair_df) else pair_df.copy()

    swapped = limited.rename(columns={"channel_a": "channel_b", "channel_b": "channel_a"})
    stacked = pd.concat([limited, swapped], ignore_index=True)

    channel_order = (
        stacked.groupby("channel_a")["shared_users"].sum().sort_values(ascending=False).index.tolist()
    )

    stacked["channel_a"] = pd.Categorical(stacked["channel_a"], categories=channel_order, ordered=True)
    stacked["channel_b"] = pd.Categorical(stacked["channel_b"], categories=channel_order[::-1], ordered=True)

    return stacked


def main() -> None:
    """Streamlitアプリのエントリーポイント."""

    st.set_page_config(page_title="チャンネル共起分析", layout="wide")
    st.title("チャンネル横断コメント分析デモ")

    st.markdown(
        """
        指定した期間内で同じユーザーが複数チャンネルにコメントした組み合わせを抽出します。
        フィルターを調整して共通視聴者を持つチャンネルの関係性を確認してください。
        """
    )

    config = _prepare_config()
    if not config:
        st.stop()

    table_name = DEFAULT_TABLE

    default_start, default_end = _default_dates()
    with st.sidebar:
        st.header("フィルター")
        start_date = st.date_input("開始日", value=default_start)
        end_date = st.date_input("終了日", value=default_end)
        if start_date > end_date:
            st.error("開始日は終了日以前である必要があります。")
            st.stop()

        start_ts, end_ts = _to_utc_range(start_date, end_date)

        try:
            channels = fetch_channels(config, table_name, start_ts, end_ts)
        except Exception as exc:  # noqa: BLE001
            st.error(f"チャンネル一覧の取得に失敗しました: {exc}")
            st.stop()

        selected_channels = st.multiselect("分析対象チャンネル", options=channels, default=channels, help="空欄の場合は期間内のすべてが対象")
        min_messages = st.number_input("ユーザーごとの最低メッセージ数", min_value=1, value=3, step=1)
        min_shared = st.number_input("チャンネル組の最低共通ユーザー数", min_value=1, value=5, step=1)
        limit = st.slider("最大表示件数", min_value=10, max_value=500, value=100, step=10)
        detail_limit = st.slider("詳細表示の最大ユーザー数", min_value=10, max_value=500, value=100, step=10)

    try:
        pair_df = fetch_channel_pairs(
            config,
            table_name,
            start_ts,
            end_ts,
            selected_channels,
            min_messages,
            min_shared,
            limit,
        )
    except Exception as exc:  # noqa: BLE001
        st.error(f"チャンネル組み合わせの取得に失敗しました: {exc}")
        st.stop()

    if pair_df.empty:
        st.info("条件に一致する共起チャンネルは見つかりませんでした。フィルターを緩めて再試行してください。")
        st.stop()

    st.subheader("共起チャンネル一覧")
    st.dataframe(pair_df, use_container_width=True)
    st.download_button(
        "CSVとしてダウンロード",
        data=_download_csv(pair_df),
        file_name="channel_pairs.csv",
        mime="text/csv",
    )

    st.subheader("共起チャンネルヒートマップ")
    max_pairs = len(pair_df)
    heatmap_limit = st.slider(
        "ヒートマップに表示する組み合わせ数",
        min_value=1,
        max_value=max_pairs,
        value=min(50, max_pairs),
        step=1,
    )
    heatmap_df = _build_heatmap_dataframe(pair_df, heatmap_limit)
    if heatmap_df.empty:
        st.info("ヒートマップに表示できるデータがありません。フィルターを調整してください。")
    else:
        chart = (
            alt.Chart(heatmap_df)
            .mark_rect()
            .encode(
                x=alt.X("channel_a:N", title="チャンネルA"),
                y=alt.Y("channel_b:N", title="チャンネルB"),
                color=alt.Color("shared_users:Q", title="共通ユーザー数", scale=alt.Scale(scheme="blues")),
                tooltip=["channel_a", "channel_b", "shared_users"],
            )
        )
        st.altair_chart(chart, use_container_width=True)

    pair_indices = list(range(len(pair_df)))
    selection = st.selectbox(
        "詳細を表示するチャンネルペアを選択",
        options=pair_indices,
        format_func=lambda idx: f"{pair_df.iloc[idx].channel_a} × {pair_df.iloc[idx].channel_b}",
    )
    selected_row = pair_df.iloc[selection]

    try:
        details_df = fetch_pair_details(
            config,
            table_name,
            start_ts,
            end_ts,
            selected_row.channel_a,
            selected_row.channel_b,
            min_messages,
            detail_limit,
        )
    except Exception as exc:  # noqa: BLE001
        st.error(f"ユーザー詳細の取得に失敗しました: {exc}")
        st.stop()

    st.subheader(f"{selected_row.channel_a} × {selected_row.channel_b} の共通ユーザー")
    if details_df.empty:
        st.info("共通ユーザーが見つかりませんでした。")
    else:
        st.dataframe(details_df, use_container_width=True)
        st.download_button(
            "ユーザー詳細をCSVダウンロード",
            data=_download_csv(details_df),
            file_name="pair_details.csv",
            mime="text/csv",
        )


if __name__ == "__main__":
    main()
try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore
