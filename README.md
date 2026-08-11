# Vietnam Law Tracking Lambda

ベトナムの法改正・制度変更に関係するニュースを毎週収集し、Geminiで日本語レポートへ変換してLINE Messaging APIで通知するAWS Lambdaです。

## 処理概要

1. EventBridgeから毎週日曜日にLambdaを起動
2. VnExpressとTuoi Tre NewsのRSSまたは一覧ページから記事を取得
3. 直近7日間の記事を対象に、英語・ベトナム語キーワードで事前絞り込み
4. 最大20件の記事をGeminiへまとめて送信
5. 日本企業、IT企業、外国人駐在員への影響を `category`、法令番号、検索キーワード付き日本語JSONで取得
6. LINE Messaging APIのPush Messageで、National Law Portalで人間が確認するための情報を含む最大5メッセージを送信
7. RSS、Rustフィルタ結果、Gemini分析結果を実行単位のMedallion JSONとして保存

## 環境変数

```text
GEMINI_API_KEY                  Gemini APIキー
GEMINI_MODEL                    使用するGeminiモデル名
LINE_CHANNEL_ACCESS_TOKEN       LINEチャネルアクセストークン
LINE_DESTINATION_ID             ユーザー、グループ、またはルームの送信先ID
LOOKBACK_DAYS                   対象日数。未設定時は7
RUST_LOG                        ログレベル。未設定時はinfo
ARTIFACT_ROOT                  Artifact保存root。未設定時はローカルがdata/runs、Lambdaが/tmp/vietnam_law_tracking/runs
```

APIキーとアクセストークンはコードやログへ出力しないでください。

## ローカルでのビルド・テスト

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test
cargo build --release
```

Lambdaとしてローカル実行する場合は、参照イベントをJSONファイルに用意して`cargo lambda invoke`を使用できます。

## Lambdaへのデプロイ

`cargo-lambda`をインストールし、Lambda用ZIPを作成します。

```bash
cargo install cargo-lambda
cargo lambda build --release --arm64 --output-format zip
```

生成された`target/lambda/vietnam_law_tracking/bootstrap.zip`をAWS Lambdaへアップロードし、ランタイムを`provided.al2023`、アーキテクチャを`arm64`、ハンドラーを`bootstrap`として設定します。環境変数はLambdaのConfigurationから設定し、Secrets ManagerまたはParameter Storeの利用を推奨します。

## EventBridge設定例

毎週日曜日の日本時間9時に起動する例です。EventBridge SchedulerのcronはUTCで指定します。

```text
cron(0 0 ? * SUN *)
```

LambdaをターゲットにしたEventBridge Schedulerを作成し、Lambdaを呼び出すIAMロールを割り当ててください。ベトナム時間の日曜日9時にする場合も同じUTC時刻です。

## LINE送信先IDの確認

LINE Developers ConsoleでMessaging APIチャネルを作成し、チャネルアクセストークンを発行します。ユーザーへ送信する場合は、Webhookで受信したイベントの`source.userId`、グループでは`source.groupId`、ルームでは`source.roomId`を確認して`LINE_DESTINATION_ID`へ設定します。Webhookを本システムへ実装する必要はありません。既存のWebhook受信環境やBotログでIDを確認してください。

## 制約・今後の対応

- データベースやS3への保存は行いません。
- 実行をまたいだ重複排除は行いません。
- ニュースサイトのHTMLやRSS仕様変更には追随が必要です。
- Geminiが報道内容を正しく解釈できない可能性があるため、重要事項は公式情報で再確認してください。
- National Law Portalの検索は自動化せず、通知に表示されたベトナム語キーワードと検索ページを使って人間が確認してください。
- Artifactは`date=YYYYMMDD/run_id=<uuid>/`配下にBronze、Silver、Goldとして保存されます。保存失敗時も本来の通知処理は継続します。
- 本番運用時には通知部分をAmazon SESへ置き換える予定です。
