# token-holder-collector

每 4 小时自动采集一组代币的持仓快照和 DexScreener 行情，并把结果提交回这个仓库。时间按北京时间（Asia/Shanghai）。

## 它做什么

- **ben**（BSC `0x3030a6d0b748c02e5543cad9200D715c7Bfb7777`）：从 Transfer 日志里发现持有人。第一次从合约创建区块扫起，之后只扫 `state/` 里记下的区块之后的新块；余额用 Multicall3 按当前链上数据读取。
- **Solana 代币**（WOULD、GIGACHAD、Catecoin、The Black Bull、ni hao cat）：优先下载 Solscan 持仓导出（前 1000 名）。Solscan 不可用时，改用 Solana 公共 RPC 的 `getProgramAccounts`，写成同样的四列。
- **DexScreener**：每个代币一份行情快照。

定时是北京时间 00:00、04:00、08:00、12:00、16:00、20:00。也可以在 GitHub 的 Actions 页面手动运行 **Collect token holders**。

某一个代币失败不会挡住其他代币。只要有失败，这次 Actions 会标成失败，但已经拿到的文件仍会提交。30 分钟内刚写过的快照会跳过，避免同一次任务被重复触发时覆盖文件。

## 文件在哪

- 持仓：`data/<文件夹>/*.csv`（UTF-8，带 BOM）
- 行情：`data/<文件夹>/dexscreener/*.csv`
- 运行日志：`data/_fallback.log`
- 增量进度：`state/`（不要删，否则 ben 会重新从创建区块扫历史）

## 在 Mac 上拉取数据

装好 Git 即可，不需要 Python。

```bash
git clone https://github.com/lnewjp88-lab/token-holder-collector.git
cd token-holder-collector
git pull
```

之后用 Excel、Numbers 或文本编辑器打开 `data/` 里的 CSV。想拿最新一份时再执行一次 `git pull`。

## 本地手动跑（可选）

```bash
pip install -r requirements.txt
python3 collect.py
```
