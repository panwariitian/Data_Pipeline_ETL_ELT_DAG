"""Data-quality trend: one chart per run history, built from dq_results.

Each bar is one pipeline run. Green = checks passed, amber = warnings, red = critical failures.
The chart is written to include/reports/ and is rebuilt by the last task of the ETL DAG.
"""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from nammamart import config as cfg  # noqa: E402

OUT = cfg.BASE / "reports" / "nammamart_dq_trend_pradeep.png"
DAG_IDS = ["nammamart_etl_pradeep", "nammamart_elt_pradeep"]
COLOURS = {"PASS": "#2e7d32", "WARN": "#f9a825", "FAIL": "#c62828"}


def build() -> dict:
    con = cfg.connect(read_only=True, wait_seconds=60)
    try:
        rows = con.execute("""SELECT dag_id, run_id, min(checked_at) AS run_at,
                                     count(*) FILTER (WHERE status = 'PASS') AS passed,
                                     count(*) FILTER (WHERE status = 'WARN') AS warned,
                                     count(*) FILTER (WHERE status = 'FAIL') AS failed
                              FROM dq_results WHERE dag_id IN (?, ?)
                              GROUP BY 1, 2 ORDER BY 3""", DAG_IDS).fetchall()
    finally:
        con.close()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(len(DAG_IDS), 1, figsize=(10, 3.2 * len(DAG_IDS)), squeeze=False)
    for ax, dag_id in zip(axes[:, 0], DAG_IDS):
        mine = [r for r in rows if r[0] == dag_id]
        labels = [r[2].strftime("%d %b %H:%M") for r in mine]
        bottom = [0] * len(mine)
        for status, col in (("PASS", 3), ("WARN", 4), ("FAIL", 5)):
            values = [r[col] for r in mine]
            ax.bar(labels, values, bottom=bottom, color=COLOURS[status], label=status)
            bottom = [b + v for b, v in zip(bottom, values)]
        ax.set_title(dag_id)
        ax.set_ylabel("checks")
        ax.tick_params(axis="x", rotation=45, labelsize=8)
        if mine:
            ax.legend(loc="upper left", fontsize=8, ncol=3)
    fig.suptitle("NammaMart data quality per run (from dq_results)")
    fig.tight_layout()
    fig.savefig(OUT, dpi=110)
    plt.close(fig)
    print(f"[dq_trend] {len(rows)} runs charted -> {OUT}")
    return {"runs": len(rows), "path": str(OUT)}
