def _true_range(h: float, l: float, prev_c: float) -> float:
    return max(h - l, abs(h - prev_c), abs(l - prev_c))


def atr(values_ohlc: list[tuple[float, float, float, float]], period: int) -> float | None:
    if len(values_ohlc) < period + 1:
        return None

    trs = []
    prev_close = values_ohlc[0][3]
    for i in range(1, len(values_ohlc)):
        _, h, l, c = values_ohlc[i]
        trs.append(_true_range(h, l, prev_close))
        prev_close = c

    last_trs = trs[-period:]
    return sum(last_trs) / period if last_trs else None
