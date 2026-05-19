# Hermes LLM 摘要任务

本项目的 Hermes 集成分两层：

- 确定性执行层：继续使用 `no_agent: true` 定时任务运行 `atrade run-pipeline ... --json`、`atrade notify ... --json` 和盘中风控。
- LLM 摘要层：只读取 `atrade ... --json` 输出和 Obsidian 报告片段，生成中文摘要、复盘和人工确认前的风险清单。
  盘前/收盘摘要需要真正的 Discord Rich Embed，因此 Hermes 脚本会在内部调用 Hermes LLM 生成正文，再通过
  `atrade notify llm-summary-card` 发送 embed；cron 自身成功输出 `[SILENT]`，避免再发一条纯文本。

LLM 摘要层不得替代评分器、买卖决策器、仓位计算器、风控闸门或交易执行。它只能解释和审计现有结果。

## 已设计任务

| 任务 | 建议时间 | Hermes 脚本 | 职责 |
| --- | --- | --- | --- |
| A股 LLM 盘前摘要 | `20 9 * * 1-5` | `a_stock_llm_morning_embed.sh` | 开盘前生成 Rich Embed：今日默认动作、数据质量、持仓风险、候选池、热门板块/热门新闻/热门股和禁止动作 |
| A股 LLM 收盘复盘 | `55 15 * * 1-5` | `a_stock_llm_close_embed.sh` | 收盘后生成 Rich Embed：流水闭环、候选池变化、人工确认、数据质量、收盘热点，以及盘前与收盘热点对比 |
| A股 LLM 周复盘补充 | `10 20 * * 0` | `a_stock_llm_weekly_context.sh` | 周报后复核系统运行质量、交易/持仓质量和信号质量 |

上下文采集能力由稳定 CLI 提供：

```bash
atrade llm-context --mode morning
atrade llm-context --mode close
atrade llm-context --mode weekly
```

Hermes 只保留 `~/.hermes/scripts/` 里的包装脚本，不进入交易系统 checkout，不设置 `--workdir`，不加载项目目录作为 agent 工作区。脚本只调用稳定 CLI：`atrade llm-context`、`hermes -z`、`atrade notify llm-summary-card`。

`atrade llm-context` 的 Markdown 输出会附带统一术语表，并把常见内部字段转成中文展示：

- `execution_allowed=false` → `自动执行：禁止`
- `proposed` → `计划已生成但不可执行`
- `candidate_pool_freshness` → `候选池新鲜度`
- `core_pool` → `核心池`
- `watch` → `观察`
- `record-buy` / `record-sell` → `买入记录命令` / `卖出记录命令`

Hermes LLM 最终发到 Discord 的正文不要裸露内部字段名、枚举值或 JSON 路径；如果必须保留协议名，格式为“中文释义（内部字段：protocol_name）”。

盘前摘要优先读取盘前流水缓存的热门板块、热门新闻和热门股；如果是周末或手动运行，没有盘前流水缓存，会退回最新可用热点缓存，并在摘要里说明这是参考数据。收盘复盘会读取盘前与收盘两组热点，并给出延续、新增、降温对比。该对比只用于复盘早盘判断质量，不作为自动交易或买入依据。

## Discord 卡片模板

`atrade llm-context --mode morning|close` 的 Markdown 输出会附带固定 Discord Markdown 卡片模板。Hermes LLM 最终回复必须保留模板标题和章节顺序，不输出原始 JSON、代码块、内部字段名或 JSON 路径。脚本再用 `atrade notify llm-summary-card` 把该 Markdown 转成 Discord Rich Embed。

LLM 最终回复必须在每个判断段落附带 `evidence_id: ...`。证据编号来自 `atrade llm-context` 输出的“证据编号清单”；没有证据编号的段落只能写“暂无可用数据”。`atrade notify llm-summary-card` 默认会在发送前校验，缺少 `evidence_id` 时返回失败并拒绝发送，避免把 AI 总结当成事实源。

盘前卡片固定顺序：

1. 系统与数据质量
2. 今日动作
3. 市场热点
4. 候选池
5. 持仓与风险
6. 今日纪律

收盘卡片固定顺序：

1. 系统与数据质量
2. 今日闭环
3. 收盘市场热点
4. 盘前 vs 收盘
5. 候选池变化
6. 持仓与风险
7. 明日清单

系统与数据质量必须放在第 1 区块，后续所有判断都要受它约束。热点只能作为市场背景和复盘线索，不能直接升级为买入意向。

卡片末尾可从内置“风控短句候选”里选 1 句，例如：

- 数据降级时，信心也要降级。
- 计划外的交易，先当风险处理。
- 观察不等于买入，热度不等于确定性。

这些短句是系统内置原创纪律提示，不接外部名言接口，不做名人归因。

## 安装脚本

```bash
mkdir -p ~/.hermes/scripts
cat > ~/.hermes/scripts/a_stock_llm_summary_embed.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
mode="${1:-}"
if [[ "$mode" != "morning" && "$mode" != "close" ]]; then
  echo "usage: a_stock_llm_summary_embed.sh morning|close" >&2
  exit 2
fi

tmp_prompt="$(mktemp)"
tmp_summary="$(mktemp)"
trap 'rm -f "$tmp_prompt" "$tmp_summary"' EXIT

if [[ "$mode" == "morning" ]]; then
  limit="1400"
else
  limit="1600"
fi

context="$(atrade llm-context --mode "$mode")"

cat > "$tmp_prompt" <<PROMPT
你是 A 股交易系统的中文审计员。只基于下方 atrade 上下文和报告片段总结，不要臆测外部事实，不要调用或建议自动调用买入/卖出记录命令。
最终只输出 Discord 卡片正文，保留上下文中“Discord 卡片输出模板”的标题和章节顺序。系统与数据质量必须是第 1 区块。
热门板块、热门新闻、热门股只作为市场背景和复盘线索，不得直接升级为买入意向。输出控制在 ${limit} 中文字以内。

${context}
PROMPT

hermes --ignore-rules -z "$(cat "$tmp_prompt")" > "$tmp_summary"

notify_args=(notify llm-summary-card --mode "$mode" --payload "$tmp_summary" --json)
if [[ "${ASTOCK_LLM_CARD_DRY_RUN:-}" == "1" ]]; then
  notify_args+=(--dry-run)
fi
send_result="$(atrade "${notify_args[@]}")"

printf '[SILENT]\n'
printf '%s\n' "$send_result"
EOF

cat > ~/.hermes/scripts/a_stock_llm_morning_embed.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/a_stock_llm_summary_embed.sh" morning
EOF

cat > ~/.hermes/scripts/a_stock_llm_close_embed.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exec "$(dirname "$0")/a_stock_llm_summary_embed.sh" close
EOF

chmod +x ~/.hermes/scripts/a_stock_llm_*_embed.sh ~/.hermes/scripts/a_stock_llm_summary_embed.sh
```

本地验证脚本链路但不发 Discord：

```bash
ASTOCK_LLM_CARD_DRY_RUN=1 ~/.hermes/scripts/a_stock_llm_morning_embed.sh
ASTOCK_LLM_CARD_DRY_RUN=1 ~/.hermes/scripts/a_stock_llm_close_embed.sh
```

脚本内部会调用一次 `hermes -z`，建议把 Hermes cron 脚本超时设置到 300 秒：

```yaml
cron:
  script_timeout_seconds: 300
```

## 创建 Hermes LLM cron

Hermes cron 原生 `deliver=discord` 只会发送普通文本，不会发送 Discord Rich Embed。盘前/收盘任务因此使用 `--no-agent` 脚本模式：脚本内部调用 `hermes -z` 生成摘要，再调用 `atrade notify llm-summary-card` 发 Rich Embed；脚本成功时输出 `[SILENT]`，阻止 cron 再发普通文本。

```bash
hermes cron create "20 9 * * 1-5" \
  "A股 LLM 盘前摘要：脚本内部调用 Hermes LLM 生成摘要，并通过 atrade notify llm-summary-card 发送 Discord Rich Embed；成功后输出 [SILENT] 防止 cron 纯文本重复投递。" \
  --name "A股 LLM 盘前摘要" \
  --deliver discord \
  --script a_stock_llm_morning_embed.sh \
  --no-agent
```

收盘复盘使用同一个创建方式，只替换 schedule、name、prompt 和 script。周复盘补充仍可保持原来的 LLM 文本摘要；如果也要 Rich Embed，再补一个 weekly wrapper。

## 直接告警仍然保留

以下信息不应等待 LLM 汇总，仍由确定性任务直接告警：

- 盘中风控
- 止损/止盈
- 人工确认
- pipeline 失败
- 核心数据源严重异常

常规状态由 LLM 摘要统一入口展示；关键风险直接报警。
