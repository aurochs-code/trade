# Hermes LLM 摘要任务

本项目的 Hermes 集成分两层：

- 确定性执行层：继续使用 `no_agent: true` 定时任务运行 `atrade run-pipeline ... --json`、`atrade notify ... --json` 和盘中风控。
- LLM 摘要层：只读取 `atrade ... --json` 输出和 Obsidian 报告片段，生成中文摘要、复盘和人工确认前的风险清单。

LLM 摘要层不得替代评分器、买卖决策器、仓位计算器、风控闸门或交易执行。它只能解释和审计现有结果。

## 已设计任务

| 任务 | 建议时间 | Hermes 脚本 | 职责 |
| --- | --- | --- | --- |
| A股 LLM 盘前摘要 | `20 9 * * 1-5` | `a_stock_llm_morning_context.sh` | 开盘前判断今日默认动作、数据质量、持仓风险、候选池和禁止动作 |
| A股 LLM 收盘复盘 | `55 15 * * 1-5` | `a_stock_llm_close_context.sh` | 收盘后总结流水闭环、候选池变化、人工确认、数据质量和明日清单 |
| A股 LLM 周复盘补充 | `10 20 * * 0` | `a_stock_llm_weekly_context.sh` | 周报后复核系统运行质量、交易/持仓质量和信号质量 |

上下文采集能力由稳定 CLI 提供：

```bash
atrade llm-context --mode morning
atrade llm-context --mode close
atrade llm-context --mode weekly
```

Hermes 只保留 `~/.hermes/scripts/` 里的薄包装脚本，不进入交易系统 checkout，不设置 `--workdir`，不加载项目目录作为 agent 工作区。

## 安装脚本

```bash
mkdir -p ~/.hermes/scripts
cat > ~/.hermes/scripts/a_stock_llm_morning_context.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exec atrade llm-context --mode morning
EOF

cat > ~/.hermes/scripts/a_stock_llm_close_context.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exec atrade llm-context --mode close
EOF

cat > ~/.hermes/scripts/a_stock_llm_weekly_context.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
exec atrade llm-context --mode weekly
EOF

chmod +x ~/.hermes/scripts/a_stock_llm_*_context.sh
```

## 创建 Hermes LLM cron

创建任务时不要传 `--no-agent`。脚本 stdout 会作为上下文注入给 Hermes agent，最终回复由 Hermes 自动投递。

```bash
hermes cron create "20 9 * * 1-5" \
  "你是 A 股交易系统的盘前中文审计员。只基于脚本输出的 atrade JSON 和报告片段总结，不要臆测外部事实，不要调用或建议自动调用 record-buy / record-sell。明确区分观察、核心池、买入意向；观察不等于买入。数据质量降级时，不要提高执行信心。输出控制在 1200 中文字以内，面向人工确认。如果没有新增可处理事项且脚本上下文也没有异常，输出 [SILENT]。" \
  --name "A股 LLM 盘前摘要" \
  --deliver discord \
  --script a_stock_llm_morning_context.sh
```

收盘复盘和周复盘补充使用同一个创建方式，只替换 schedule、name、prompt 和 script。

## 直接告警仍然保留

以下信息不应等待 LLM 汇总，仍由确定性任务直接告警：

- 盘中风控
- 止损/止盈
- 人工确认
- pipeline 失败
- 核心数据源严重异常

常规状态由 LLM 摘要统一入口展示；关键风险直接报警。
