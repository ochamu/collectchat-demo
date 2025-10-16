from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import logging
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import yt_dlp
import clickhouse_connect
from yt_dlp.extractor.youtube import _base as youtube_base
from yt_dlp.utils import traverse_obj


@dataclass
class ChatMessage:
    """YouTubeチャットの1件分の情報を保持するデータクラス。"""

    video_id: str
    timestamp: dt.datetime
    offset_ms: Optional[int]
    author_name: str
    author_channel_id: Optional[str]
    message: str
    message_type: str
    channel_name: Optional[str] = None
    video_title: Optional[str] = None


def configure_logging(verbosity: int) -> None:
    """ログ設定を初期化する。"""

    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format="%(levelname)s: %(message)s")


class YouTubeChatChunkFetcher:
    """YouTubeライブチャットをチャンク単位で取得するヘルパー。"""

    def __init__(
        self,
        youtube_url: str,
        chunk_duration_sec: int,
        start_offset_sec: int,
        end_offset_sec: Optional[int],
    ) -> None:
        if chunk_duration_sec <= 0:
            raise ValueError("チャンク秒数は1以上で指定してください")
        self.youtube_url = youtube_url
        self.chunk_duration_ms = chunk_duration_sec * 1000
        self.start_offset_ms = max(0, start_offset_sec * 1000)
        self.end_offset_ms = end_offset_sec * 1000 if end_offset_sec is not None else None
        self.video_id: Optional[str] = None
        self.channel_name: Optional[str] = None
        self.video_title: Optional[str] = None

    def iter_chunks(self) -> Iterator[List[ChatMessage]]:
        """ライブチャットを取得し、一定秒数ごとのチャンクとして返す。"""

        with yt_dlp.YoutubeDL({"quiet": True, "no_warnings": True}) as ydl:
            info = ydl.extract_info(self.youtube_url, download=False)
            video_id = info.get("id")
            if not video_id:
                raise RuntimeError("動画IDが取得できませんでした")
            self.video_id = video_id
            self.channel_name = _first_not_none(info.get("channel"), info.get("uploader"))
            self.video_title = info.get("title")
            watch_url = info.get("webpage_url") or self.youtube_url
            logging.info("動画ID: %s", video_id)
            logging.debug("watch URL: %s", watch_url)
            watch_html = ydl.urlopen(watch_url).read().decode("utf-8", "replace")
            ie = youtube_base.YoutubeBaseInfoExtractor(ydl)
            initial_data = ie.extract_yt_initial_data(video_id, watch_html)
            ytcfg = ie.extract_ytcfg(video_id, watch_html)

        continuation = traverse_obj(
            initial_data,
            (
                "contents",
                "twoColumnWatchNextResults",
                "conversationBar",
                "liveChatRenderer",
                "continuations",
                0,
                "reloadContinuationData",
                "continuation",
            ),
        )
        if not continuation:
            raise RuntimeError("ライブチャットの継続トークンが取得できませんでした")

        api_key = traverse_obj(ytcfg, ("INNERTUBE_API_KEY",))
        context = traverse_obj(ytcfg, ("INNERTUBE_CONTEXT",))
        visitor_data = traverse_obj(context, ("client", "visitorData"))
        if not api_key or not context:
            raise RuntimeError("YouTube API情報の抽出に失敗しました")

        headers = ie.generate_api_headers(ytcfg=ytcfg, visitor_data=visitor_data)
        headers["content-type"] = "application/json"

        base_context = context
        api_url = f"https://www.youtube.com/youtubei/v1/live_chat/get_live_chat_replay?key={api_key}"

        next_continuation = continuation
        click_tracking_params: Optional[str] = None
        last_offset_ms: Optional[int] = None
        chunk_messages: List[ChatMessage] = []
        chunk_start_ms = self.start_offset_ms
        chunk_end_ms = chunk_start_ms + self.chunk_duration_ms

        while next_continuation:
            payload_context = copy.deepcopy(base_context)
            if click_tracking_params:
                payload_context.setdefault("clickTracking", {})["clickTrackingParams"] = click_tracking_params
            request_payload: Dict[str, Any] = {
                "context": payload_context,
                "continuation": next_continuation,
            }
            if last_offset_ms is not None:
                request_payload["currentPlayerState"] = {
                    "playerOffsetMs": str(max(last_offset_ms - 5000, 0))
                }

            try:
                response = self._post_json(api_url, headers, request_payload)
            except urllib.error.URLError as exc:  # noqa: PERF203
                raise RuntimeError(f"ライブチャット取得で通信エラーが発生しました: {exc}") from exc

            live_chat = traverse_obj(
                response,
                ("continuationContents", "liveChatContinuation"),
                default={},
            )
            actions = live_chat.get("actions") or []
            messages = collect_messages(self.video_id, {"actions": actions})

            for message in messages:
                if message.channel_name is None:
                    message.channel_name = self.channel_name
                if message.video_title is None:
                    message.video_title = self.video_title
                offset = message.offset_ms
                if offset is None:
                    offset = last_offset_ms if last_offset_ms is not None else 0
                    message.offset_ms = offset
                if last_offset_ms is None or offset > last_offset_ms:
                    last_offset_ms = offset
                if offset < self.start_offset_ms:
                    continue
                if self.end_offset_ms is not None and offset > self.end_offset_ms:
                    if chunk_messages:
                        yield chunk_messages
                    return
                while offset >= chunk_end_ms:
                    if chunk_messages:
                        yield chunk_messages
                    chunk_messages = []
                    chunk_start_ms = chunk_end_ms
                    chunk_end_ms = chunk_start_ms + self.chunk_duration_ms
                chunk_messages.append(message)

            continuation_entry = next(
                (
                    entry.get("liveChatReplayContinuationData")
                    for entry in live_chat.get("continuations", [])
                    if isinstance(entry, dict) and entry.get("liveChatReplayContinuationData")
                ),
                None,
            )
            if continuation_entry:
                next_continuation = continuation_entry.get("continuation")
                click_tracking_params = continuation_entry.get("clickTrackingParams")
            else:
                next_continuation = None

            if self.end_offset_ms is not None and last_offset_ms is not None:
                if last_offset_ms >= self.end_offset_ms:
                    break

        if chunk_messages:
            yield chunk_messages

    @staticmethod
    def _post_json(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
        """JSONをPOSTして辞書に変換する。"""

        request = urllib.request.Request(
            url,
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
        )
        with urllib.request.urlopen(request) as response:
            return json.loads(response.read().decode("utf-8"))


def collect_messages(video_id: str, payload: Dict[str, Any]) -> List[ChatMessage]:
    """チャットJSONからメッセージを抽出する。"""

    raw_events: Sequence[Any] = _extract_event_sequence(payload)
    messages: List[ChatMessage] = []
    for raw_event in raw_events:
        offset_ms = _extract_offset_ms(raw_event)
        for renderer_key, renderer in _iter_renderers(raw_event):
            message = _build_message(video_id, renderer, renderer_key, raw_event, offset_ms)
            if message is not None:
                messages.append(message)

    filtered = [m for m in messages if m.timestamp is not None]
    if not filtered:
        logging.warning("有効なチャットメッセージが抽出できませんでした")
        return []

    filtered.sort(key=lambda item: (item.offset_ms if item.offset_ms is not None else sys.maxsize, item.timestamp))
    base_timestamp = filtered[0].timestamp
    for msg in filtered:
        if msg.offset_ms is None:
            delta = msg.timestamp - base_timestamp
            msg.offset_ms = max(0, int(delta.total_seconds() * 1000))
    return filtered


def _extract_event_sequence(payload: Dict[str, Any]) -> Sequence[Any]:
    """チャットイベントが格納された配列を返す。"""

    for key in ("events", "actions", "replayActions"):
        if key in payload and isinstance(payload[key], list):
            return payload[key]
    raise RuntimeError("チャットイベント配列が見つかりませんでした")


def _extract_offset_ms(event: Dict[str, Any]) -> Optional[int]:
    """イベントから相対ミリ秒を取り出す。"""

    candidates = [
        event.get("offsetMs"),
        event.get("videoOffsetTimeMsec"),
    ]
    replay = event.get("replayChatItemAction")
    if isinstance(replay, dict):
        candidates.append(replay.get("videoOffsetTimeMsec"))
    for candidate in candidates:
        if candidate is None:
            continue
        try:
            return int(float(candidate))
        except (TypeError, ValueError):
            continue
    return None


def _iter_renderers(event: Any) -> Iterator[Tuple[Optional[str], Dict[str, Any]]]:
    """イベント内のRenderer風オブジェクトを走査する。"""

    stack: List[Tuple[Any, Optional[str]]] = [(event, None)]
    while stack:
        current, current_key = stack.pop()
        if isinstance(current, dict):
            if _is_chat_renderer_candidate(current, current_key):
                yield current_key, current
            for key, value in current.items():
                stack.append((value, key))
        elif isinstance(current, list):
            for item in current:
                stack.append((item, current_key))


def _is_chat_renderer_candidate(node: Dict[str, Any], key: Optional[str]) -> bool:
    """チャットメッセージを保持するノードか判定する。"""

    if "message" in node and "timestampUsec" in node:
        if key and key.endswith("Renderer"):
            return True
        if not key:
            return True
    return False


def _build_message(
    video_id: str,
    renderer: Dict[str, Any],
    renderer_key: Optional[str],
    event: Dict[str, Any],
    offset_ms: Optional[int],
) -> Optional[ChatMessage]:
    """RendererノードからChatMessageを生成する。"""

    timestamp_usec = _first_not_none(
        renderer.get("timestampUsec"),
        event.get("timestampUsec"),
    )
    if not timestamp_usec:
        return None
    try:
        timestamp = dt.datetime.fromtimestamp(int(timestamp_usec) / 1_000_000, tz=dt.timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None

    message_text = _extract_message_text(renderer)
    if not message_text:
        return None

    author_name = _first_not_none(
        _simple_text(renderer.get("authorName")),
        _simple_text(renderer.get("author", {}).get("name")),
        event.get("author", {}).get("name", {}).get("simpleText"),
    ) or ""

    author_channel_id = _first_not_none(
        renderer.get("authorExternalChannelId"),
        renderer.get("author", {}).get("id"),
        event.get("authorExternalChannelId"),
        event.get("author", {}).get("channelId"),
    )

    message_type = (renderer_key or "unknown").replace("Renderer", "") or "unknown"

    return ChatMessage(
        video_id=video_id,
        timestamp=timestamp,
        offset_ms=offset_ms,
        author_name=author_name,
        author_channel_id=author_channel_id,
        message=message_text,
        message_type=message_type,
    )


def _extract_message_text(renderer: Dict[str, Any]) -> Optional[str]:
    """Rendererからユーザー向けのテキストを抽出する。"""

    message_runs = renderer.get("message", {}).get("runs")
    text = _join_runs(message_runs)
    if text:
        return text
    header_runs = renderer.get("headerSubtext", {}).get("runs")
    text = _join_runs(header_runs)
    if text:
        return text
    purchase_text = renderer.get("purchaseAmountText")
    text = _simple_text(purchase_text)
    if text:
        return text
    return None


def _join_runs(runs: Any) -> Optional[str]:
    """YouTubeのruns配列を連結して文字列化する。"""

    if not isinstance(runs, list):
        return None
    parts: List[str] = []
    for item in runs:
        if not isinstance(item, dict):
            continue
        if "text" in item:
            parts.append(str(item["text"]))
        elif "emoji" in item:
            emoji = item["emoji"]
            shortcuts = emoji.get("shortcuts")
            if shortcuts:
                parts.append(shortcuts[0])
            else:
                parts.append(emoji.get("emojiId", ""))
    if not parts:
        return None
    return "".join(parts)


def _simple_text(node: Any) -> Optional[str]:
    """simpleText形式の辞書から文字列を取り出す。"""

    if isinstance(node, dict) and "simpleText" in node:
        return str(node["simpleText"])
    if isinstance(node, str):
        return node
    return None


def _first_not_none(*values: Any) -> Any:
    """None以外の最初の値を返す。"""

    for value in values:
        if value is not None:
            return value
    return None


class ClickHouseWriter:
    """ClickHouseへの書き込みを担うユーティリティ。"""

    def __init__(
        self,
        host: str,
        port: int,
        user: str,
        password: Optional[str],
        database: str,
        secure: bool,
        settings: Optional[Dict[str, Any]] = None,
    ) -> None:
        # clickhouse-connectのクライアントを初期化
        self._client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            secure=secure,
            database=database,
            **(settings or {}),
        )
        self._database = database

    def ensure_table(self, table: str) -> None:
        """必要なテーブルを作成する。"""

        validated_table = _validate_table_name(table)
        create_sql = f"""
            CREATE TABLE IF NOT EXISTS {self._database}.{validated_table} (
                video_id String,
                message_timestamp DateTime64(6, 'UTC'),
                offset_ms UInt64,
                author_name String,
                author_channel_id Nullable(String),
                channel_name Nullable(String),
                video_title String,
                message String,
                message_type LowCardinality(String)
            )
            ENGINE = MergeTree
            ORDER BY (video_id, message_timestamp, offset_ms)
        """
        # SQL内の改行やスペースをまとめて整形
        compact_sql = " ".join(create_sql.split())
        logging.debug("テーブル作成SQL: %s", compact_sql)
        self._client.command(compact_sql)
        self._ensure_column(validated_table, "channel_name Nullable(String)", after="author_channel_id")
        self._ensure_column(validated_table, "video_title String", after="channel_name")

    def insert_messages(self, table: str, messages: Sequence[ChatMessage]) -> None:
        """チャットメッセージを一括インサートする。"""

        if not messages:
            logging.info("挿入対象のメッセージがありません")
            return
        validated_table = _validate_table_name(table)
        payload = [
            (
                row.video_id,
                row.timestamp.astimezone(dt.timezone.utc).replace(tzinfo=None),
                int(row.offset_ms or 0),
                row.author_name,
                row.author_channel_id,
                row.channel_name,
                row.video_title or "",
                row.message,
                row.message_type,
            )
            for row in messages
        ]
        logging.info("ClickHouseへ%d件のメッセージを送信します", len(payload))
        self._client.insert(
            f"{self._database}.{validated_table}",
            payload,
            column_names=(
                "video_id",
                "message_timestamp",
                "offset_ms",
                "author_name",
                "author_channel_id",
                "channel_name",
                "video_title",
                "message",
                "message_type",
            ),
        )

    def _ensure_column(self, table: str, column_def: str, after: Optional[str] = None) -> None:
        """テーブルに列が存在しなければ追加する。"""

        after_clause = f" AFTER {after}" if after else ""
        sql = f"ALTER TABLE {self._database}.{table} ADD COLUMN IF NOT EXISTS {column_def}{after_clause}"
        logging.debug("列追加チェックSQL: %s", " ".join(sql.split()))
        self._client.command(sql)


def _validate_table_name(table: str) -> str:
    """テーブル名を安全に扱うための検証を行う。"""

    if not table:
        raise ValueError("テーブル名が指定されていません")
    if not table.replace("_", "").isalnum():
        raise ValueError("テーブル名には英数字とアンダースコアのみ使用できます")
    return table


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """コマンドライン引数を解析する。"""

    parser = argparse.ArgumentParser(description="YouTubeライブチャットをClickHouseへ保存するツール")
    parser.add_argument("youtube_url", help="YouTubeライブまたはアーカイブのURL")
    parser.add_argument("--host", default="localhost", help="ClickHouseホスト名")
    parser.add_argument("--port", type=int, default=9000, help="ClickHouseポート番号")
    parser.add_argument("--user", default="default", help="ClickHouseユーザー")
    parser.add_argument("--password", default=None, help="ClickHouseパスワード")
    parser.add_argument("--database", default="default", help="ClickHouseデータベース名")
    parser.add_argument("--table", default="youtube_live_chat", help="保存先テーブル名")
    parser.add_argument("--secure", action="store_true", help="TLS接続を利用する場合に指定")
    parser.add_argument("--verbosity", "-v", action="count", default=0, help="-vでINFO、-vvでDEBUGログを有効化")
    parser.add_argument("--chunk-duration", type=int, default=600, help="チャットを区切る秒数 (デフォルト600秒)")
    parser.add_argument("--start-offset", type=int, default=0, help="取得開始位置 (秒)")
    parser.add_argument("--end-offset", type=int, default=None, help="取得終了位置 (秒、省略可)")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    """エントリーポイント。"""

    args = parse_args(argv)
    configure_logging(args.verbosity)

    try:
        fetcher = YouTubeChatChunkFetcher(
            youtube_url=args.youtube_url,
            chunk_duration_sec=args.chunk_duration,
            start_offset_sec=args.start_offset,
            end_offset_sec=args.end_offset,
        )
    except Exception as exc:  # noqa: BLE001
        logging.error("チャット取得設定の初期化に失敗しました: %s", exc)
        return 1

    writer = ClickHouseWriter(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        secure=args.secure,
    )

    try:
        writer.ensure_table(args.table)
    except Exception as exc:  # noqa: BLE001
        logging.error("ClickHouseテーブルの準備に失敗しました: %s", exc)
        return 1

    total_inserted = 0
    try:
        for idx, chunk in enumerate(fetcher.iter_chunks(), start=1):
            if not chunk:
                continue
            try:
                writer.insert_messages(args.table, chunk)
            except Exception as insert_exc:  # noqa: BLE001
                logging.error("チャンク%dの挿入に失敗しました: %s", idx, insert_exc)
                return 1
            total_inserted += len(chunk)
            start_sec = (chunk[0].offset_ms or 0) / 1000
            end_sec = (chunk[-1].offset_ms or 0) / 1000
            logging.info(
                "チャンク%dを挿入しました (%.2f秒～%.2f秒, %d件)",
                idx,
                start_sec,
                end_sec,
                len(chunk),
            )
    except Exception as exc:  # noqa: BLE001
        logging.error("チャットデータの取得・挿入でエラーが発生しました: %s", exc)
        return 1

    if total_inserted == 0:
        logging.warning("対象範囲のチャットメッセージが見つかりませんでした")
    else:
        logging.info("合計%d件のメッセージを挿入しました", total_inserted)
    return 0


if __name__ == "__main__":
    sys.exit(main())
