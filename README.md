# ClickHouseデモ
[デモページ](https://collectchat-demo-jasqkxrzsmbfx6grxw3zxo.streamlit.app/)

## テーブル詳細

- video_id: 動画ID（String型）
- message_timestamp: メッセージのタイムスタンプ（DateTime64型、UTCタイムゾーン）
- offset_ms: オフセット時間（ミリ秒、UInt64型）
- author_name: 作成者名（String型）
- author_channel_id: 作成者チャンネルID（Nullable String型）
- channel_name: チャンネル名（Nullable String型）
- video_title: 動画タイトル（String型）
- message: メッセージ内容（String型）
- message_type: メッセージタイプ（LowCardinality String型）

### レコード数

195,087件
