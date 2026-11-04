# -*- coding: utf-8 -*-
"""诊断：OKX rubik taker-volume 的字段顺序与时间戳对齐口径。
这决定"主动买卖失衡"因子的方向，必须用数据判定，不能猜。
"""
import json
import math
import sys
import urllib.request

B = "https://www.okx.com/api/v5"


def g(u):
    return json.loads(urllib.request.urlopen(
        urllib.request.Request(u, headers={"User-Agent": "x"}), timeout=25).read().decode())["data"]


def corr(xs, ys):
    n = len(xs)
    if n < 5:
        return float("nan")
    mx, my = sum(xs) / n, sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = math.sqrt(sum((x - mx) ** 2 for x in xs))
    vy = math.sqrt(sum((y - my) ** 2 for y in ys))
    return cov / (vx * vy) if vx and vy else float("nan")


def show(title, seq):
    ts = sorted(seq)
    print(f"  {title}: {len(ts)} 行, 首={ts[0]}, 尾={ts[-1]}")


for inst, ccy in [("SOL-USDT-SWAP", "SOL"), ("BTC-USDT-SWAP", "BTC")]:
    print("=" * 76)
    print(f"{inst}")
    for bar, period in [("5m", "5m"), ("1H", "1H")]:
        c = {int(v[0]): v for v in g(f"{B}/market/candles?instId={inst}&bar={bar}&limit=300")}
        t = {int(v[0]): v for v in g(f"{B}/rubik/stat/taker-volume?ccy={ccy}&instType=CONTRACTS&period={period}")}
        print(f"-- bar={bar} period={period}")
        show("K线", c)
        show("taker", t)
        inter = sorted(set(c) & set(t))
        print(f"  精确交集 = {len(inter)}")
        if len(inter) < 10:
            continue
        # 按 lag 对齐：t 的 ts 与 c 的 ts 相差 lag 个 bar
        step = 300_000 if bar == "5m" else 3_600_000
        for lag in (-2, -1, 0, 1, 2):
            xs, ys = [], []
            for k in inter:
                kk = k + lag * step
                if kk in c:
                    col1, col2 = float(t[k][1]), float(t[k][2])
                    ret = math.log(float(c[kk][4]) / float(c[kk][1]))
                    xs.append(col1 - col2)          # 假设: col1=买, col2=卖
                    ys.append((ret, col2 - col1))   # 备选
            if len(xs) < 10:
                continue
            rets = [y[0] for y in ys]
            print(f"   lag={lag:+d}  n={len(xs):3d}  "
                  f"corr(col1-col2, 收益)={corr(xs, rets):+.4f}   "
                  f"corr(col2-col1, 收益)={corr([y[1] for y in ys], rets):+.4f}")
        # 单位确认
        r = [(float(t[k][1]) + float(t[k][2])) / float(c[k][7]) for k in inter if float(c[k][7])]
        r.sort()
        if r:
            print(f"   (col1+col2)/K线USDT成交额 中位={r[len(r)//2]:.4f}  -> 接近1即单位=USDT")
    print()
